package main

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/term"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"gopkg.in/yaml.v3"
)

const SchemaVersion = 2

type (
	Config struct {
		Repos []*RepoConfig
	}

	RepoConfig struct {
		Name  string
		Url   string
		GoMod string
	}

	// vvv --- persisted --- vvv

	State struct {
		Repos         map[string]*RepoState
		Modified      bool
		SchemaVersion int
	}

	RepoState struct {
		Tags    map[string]plumbing.Hash
		Commits map[plumbing.Hash]CommitInfo
	}

	CommitInfo struct {
		Parents []plumbing.Hash
		Title   string
		Deps    map[string]plumbing.Hash
	}

	// ^^^ --- persisted --- ^^^

	whatrel struct {
		cfg     Config
		st      State
		repos   map[string]*repo
		byGoMod map[string]*repo
	}

	repo struct {
		cfg           *RepoConfig
		st            *RepoState
		git           *git.Repository
		refreshedRepo bool
		newTags       bool
	}
)

var (
	// tagAge          = flag.Int("age", 30, "how many days back to list tags")
	refreshInterval = flag.Duration("refresh_interval", time.Hour, "how often to refresh git repo cache")
	resetState      = flag.Bool("reset", false, "")

	versionTagRe = regexp.MustCompile("^v[0-9]")
	digitsRe     = regexp.MustCompile("^[0-9]+$")
)

func fatalIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func must[T any](t T, err error) T {
	fatalIfErr(err)
	return t
}

var _re_submpath = regexp.MustCompile(`^\[submodule "([^"]+)"\]$`)
var _re_submurl = regexp.MustCompile(`url\s*=\s*(\S*)`)

func parseGitModules(contents string) (map[string]string, error) {
	out := make(map[string]string) // path -> url
	var path string
	for _, line := range strings.Split(contents, "\n") {
		if m := _re_submpath.FindStringSubmatch(line); m != nil {
			path = m[1]
		} else if m := _re_submurl.FindStringSubmatch(line); m != nil {
			if path == "" {
				return nil, errors.New("url without path")
			}
			out[path] = m[1]
		}
	}
	return out, nil
}

func (w *whatrel) loadRepo(cacheBase string, r *repo) {
	dir := filepath.Join(cacheBase, "repo-"+r.cfg.Name)
	if st, err := os.Stat(dir); err == nil {
		// have already
		r.git = must(git.PlainOpen(dir))
		if r.refreshedRepo = time.Since(st.ModTime()) > *refreshInterval; r.refreshedRepo {
			// too old, refresh
			log.Println("fetching", r.cfg.Name)
			fatalIfErr(r.git.Fetch(&git.FetchOptions{
				Tags:     git.AllTags,
				Prune:    true,
				Progress: os.Stdout,
			}))
			// mark mtime
			os.Chtimes(dir, time.Now(), time.Now())
		}
		return
	} else if !os.IsNotExist(err) {
		fatalIfErr(err)
	}

	// do initial clone
	log.Println("initial clone of", r.cfg.Name)
	r.git = must(git.PlainClone(dir, true, &git.CloneOptions{
		URL:      r.cfg.Url,
		Mirror:   true,
		Progress: os.Stdout,
	}))
	r.refreshedRepo = true
}

func (w *whatrel) loadTags(r *repo) {
	if !r.refreshedRepo && !*resetState {
		return
	}
	log.Println("loading tags on", r.cfg.Name)
	must(r.git.Tags()).ForEach(func(ref *plumbing.Reference) error {
		tagName := strings.TrimPrefix(ref.Name().String(), "refs/tags/")
		commit := ref.Hash()
		if !versionTagRe.MatchString(tagName) {
			return nil
		}
		if _, ok := r.st.Tags[tagName]; ok {
			return nil
		}
		if tagObj, err := r.git.TagObject(commit); err == nil {
			commit = tagObj.Target
		}
		r.st.Tags[tagName] = commit
		r.newTags = true
		w.st.Modified = true
		log.Println("new tag", tagName)
		return nil
	})
}

func (w *whatrel) loadCommits(r *repo) {
	if !r.newTags && !*resetState {
		return
	}
	log.Println("loading commits on", r.cfg.Name)
	for _, commit := range r.st.Tags {
		w.loadCommit(r, commit)
	}
}

func (w *whatrel) loadCommit(r *repo, commit plumbing.Hash) {
	if _, ok := r.st.Commits[commit]; ok {
		return
	}

	c, err := r.git.CommitObject(commit)
	if err != nil {
		log.Printf("can't find commit %s in %s", commit, r.cfg.Name)
		return
	}

	title, _, _ := strings.Cut(c.Message, "\n")

	parents := make([]plumbing.Hash, c.NumParents())
	for i := range parents {
		parents[i] = must(c.Parent(i)).Hash
	}

	// get go.mod if present
	deps := make(map[string]plumbing.Hash)
	if f, err := c.File("go.mod"); err == nil {
		if mod, err := modfile.ParseLax("go.mod", []byte(must(f.Contents())), nil); err == nil {
			for _, req := range mod.Require {
				p, v := req.Mod.Path, req.Mod.Version
				// find in state
				if depR := w.byGoMod[p]; depR != nil {
					if v == "" {
						log.Printf("version for %s in %s is empty", depR.cfg.Name, r.cfg.Name)
					} else if module.IsPseudoVersion(v) {
						rev := must(module.PseudoVersionRev(v))
						revBytes := must(hex.DecodeString(rev))
						hashes := must(depR.git.Storer.(*filesystem.Storage).HashesWithPrefix(revBytes))
						if len(hashes) == 1 {
							deps[depR.cfg.Name] = hashes[0]
						} else {
							log.Printf("version for %s in %s is %s, but found %d hashes", depR.cfg.Name, r.cfg.Name, v, len(hashes))
						}
					} else {
						if hash, ok := depR.st.Tags[v]; ok {
							deps[depR.cfg.Name] = hash
						} else {
							log.Printf("version for %s in %s is %s, but is unknown tag", depR.cfg.Name, r.cfg.Name, v)
						}
					}
				}
			}
		} else {
			log.Println("error parsing go.mod from commit", c.Hash)
		}
	}

	if f, err := c.File(".gitmodules"); err == nil {
		if mods, err := parseGitModules(must(f.Contents())); err == nil {
			for path, url := range mods {
				if depR := w.findByUrl(url); depR != nil {
					deps[depR.cfg.Name] = must(must(c.Tree()).FindEntry(path)).Hash
				}
			}
		} else {
			log.Println("error parsing .gitmodules from commit", c.Hash)
		}
	}

	r.st.Commits[commit] = CommitInfo{
		Title:   title,
		Parents: parents,
		Deps:    deps,
	}
	for _, commit := range parents {
		w.loadCommit(r, commit)
	}
	for depName, commit := range deps {
		w.loadCommit(w.repos[depName], commit)
	}
}

func (w *whatrel) findByUrl(url string) *repo {
	url = strings.TrimSuffix(url, ".git")
	for _, r := range w.repos {
		if strings.TrimSuffix(r.cfg.Url, ".git") == url {
			return r
		}
	}
	return nil
}

func (w *whatrel) findTags(arg string) {
	// form: repo#pr or repo#text
	name, pr, found := strings.Cut(arg, "#")
	if !found {
		log.Fatalln("arg must be in form repo#pr or repo#text")
	}

	var matchTitle func(string) bool
	if digitsRe.MatchString(pr) {
		suffix := " (#" + pr + ")"
		matchTitle = func(title string) bool {
			return strings.HasSuffix(title, suffix)
		}
	} else {
		matchTitle = func(title string) bool {
			return strings.Contains(title, pr)
		}
	}

	type key struct {
		c plumbing.Hash
		n [12]byte
	}
	cache := make(map[key]bool, 10000)

	var checkCommit func(r *repo, c plumbing.Hash) bool
	checkCommit = func(r *repo, c plumbing.Hash) bool {
		k := key{c: c}
		copy(k.n[:], r.cfg.Name)

		if val, ok := cache[k]; ok {
			return val
		}
		ci := r.st.Commits[c]
		if r.cfg.Name == name && matchTitle(ci.Title) {
			cache[k] = true
			return true
		}
		val := false
		for _, p := range ci.Parents {
			if checkCommit(r, p) {
				val = true
				break
			}
		}
		for depName, depC := range ci.Deps {
			if checkCommit(w.repos[depName], depC) {
				val = true
				break
			}
		}
		cache[k] = val
		return val
	}

	width, _, err := term.GetSize(1)
	if err != nil {
		width = 80
	}

	fmt.Printf("%s is in:\n", arg)
	for _, r := range w.repos {
		var tags []string
		maxLen := 0
		const pad = 2
		for tag, commit := range r.st.Tags {
			if checkCommit(r, commit) {
				tags = append(tags, tag)
				maxLen = max(maxLen, len(tag))
			}
		}
		if maxLen > 0 {
			fmt.Printf("  repo: %s\n", r.cfg.Name)
			cols := max(1, (width-4)/(maxLen+pad))
			slices.Sort(tags)
			os.Stdout.WriteString("    ")
			for i, t := range tags {
				os.Stdout.WriteString(t + strings.Repeat(" ", maxLen-len(t)+pad))
				if (i+1)%cols == 0 && i != len(tags)-1 {
					os.Stdout.WriteString("\n    ")
				}
			}
			os.Stdout.WriteString("\n")
		}
	}
}

func (w *whatrel) persistState(stateFile string) {
	if !w.st.Modified {
		return
	}
	cacheBase := filepath.Dir(stateFile)
	tmpfile := must(os.CreateTemp(cacheBase, "state.gob.tmp"))
	defer os.Remove(tmpfile.Name())
	fatalIfErr(gob.NewEncoder(tmpfile).Encode(w.st))
	fatalIfErr(tmpfile.Close())
	fatalIfErr(os.Rename(tmpfile.Name(), stateFile))
}

func main() {
	flag.Parse()

	userConfigDir := must(os.UserConfigDir())
	userCacheDir := must(os.UserCacheDir())
	cacheBase := filepath.Join(userCacheDir, "whatrel")

	configFile := filepath.Join(userConfigDir, "whatrel.yaml")
	cfgBytes := must(os.ReadFile(configFile))

	var w whatrel

	fatalIfErr(yaml.Unmarshal(cfgBytes, &w.cfg))

	stateFile := filepath.Join(cacheBase, "state.gob")
	stateBytes, err := os.ReadFile(stateFile)
	if err == nil {
		fatalIfErr(gob.NewDecoder(bytes.NewReader(stateBytes)).Decode(&w.st))
	} else if !os.IsNotExist(err) {
		fatalIfErr(err)
	}

	if w.st.SchemaVersion != SchemaVersion || *resetState {
		// schema changed, reset state
		w.st = State{
			SchemaVersion: SchemaVersion,
		}
	}
	if w.st.Repos == nil {
		w.st.Repos = make(map[string]*RepoState)
	}

	// write out if modified
	w.st.Modified = false
	defer w.persistState(stateFile)

	w.repos = make(map[string]*repo)
	w.byGoMod = make(map[string]*repo)
	for _, r := range w.cfg.Repos {
		rst := w.st.Repos[r.Name]
		if rst == nil {
			rst = &RepoState{
				Tags:    make(map[string]plumbing.Hash),
				Commits: make(map[plumbing.Hash]CommitInfo),
			}
			w.st.Repos[r.Name] = rst
		}
		repo := &repo{cfg: r, st: rst}
		w.repos[r.Name] = repo
		if r.GoMod != "" {
			w.byGoMod[r.GoMod] = repo
		}
	}
	for _, r := range w.repos {
		w.loadRepo(cacheBase, r)
	}
	for _, r := range w.repos {
		w.loadTags(r)
	}
	for _, r := range w.repos {
		w.loadCommits(r)
	}

	for _, arg := range flag.Args() {
		w.findTags(arg)
	}
}
