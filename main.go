package main

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/term"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"gopkg.in/yaml.v3"
)

const SchemaVersion = 4

type (
	Config struct {
		Repos         map[string]*RepoConfig `yaml:"repos"`
		IgnoreDeps    []string               `yaml:"ignoreDeps"`
		DeployCommand string                 `yaml:"deployCommand"`
	}

	RepoConfig struct {
		Url       string `yaml:"url"`
		DeployKey string `yaml:"deployKey"`
	}

	// vvv --- persisted --- vvv

	State struct {
		SchemaVersion int
		Repos         map[string]*RepoState
		GoModMap      map[string]string // go mod name -> repo name
		Modified      bool
	}

	RepoState struct {
		Tags    map[string]plumbing.Hash
		Commits map[plumbing.Hash]CommitInfo
	}

	CommitInfo struct {
		Title   string
		CTime   int64
		Parents []plumbing.Hash
		Deps    map[string]plumbing.Hash
	}

	// ^^^ --- persisted --- ^^^

	whatrel struct {
		cfg        Config
		st         State
		repos      map[string]*repo
		ignoreDeps map[plumbing.Hash]struct{}
		width      int
	}

	repo struct {
		name          string
		cfg           *RepoConfig
		st            *RepoState
		git           *git.Repository
		refreshedRepo bool
		newTags       bool
	}

	commit struct {
		repo string
		ci   CommitInfo
		hash plumbing.Hash
	}
)

var (
	// tagAge          = flag.Int("age", 30, "how many days back to list tags")
	refreshInterval = flag.Duration("refresh_interval", time.Hour, "how often to refresh git repo cache")
	deploy          = flag.Bool("deploy", false, "find deployments instead of tags")
	resetState      = flag.Bool("reset", false, "")
	dumpState       = flag.Bool("dump", false, "")

	versionTagRe = regexp.MustCompile("^v[0-9]")
	digitsRe     = regexp.MustCompile("^[0-9]+$")
	subModPathRe = regexp.MustCompile(`^\[submodule "([^"]+)"\]$`)
	subModUrlRe  = regexp.MustCompile(`url\s*=\s*(\S*)`)
	hashRe       = regexp.MustCompile(`^[0-9a-f]+$`)
)

func fatalIfErr(err error) {
	if err != nil {
		panic(err)
		log.Fatal(err)
	}
}

func must[T any](t T, err error) T {
	fatalIfErr(err)
	return t
}

func parseGitModules(contents string) (map[string]string, error) {
	out := make(map[string]string) // path -> url
	var path string
	for _, line := range strings.Split(contents, "\n") {
		if m := subModPathRe.FindStringSubmatch(line); m != nil {
			path = m[1]
		} else if m := subModUrlRe.FindStringSubmatch(line); m != nil {
			if path == "" {
				return nil, errors.New("url without path")
			}
			out[path] = m[1]
		}
	}
	return out, nil
}

func (c *commit) String() string {
	t := time.Unix(c.ci.CTime, 0).Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%s  %x  %s  \u00ab%s\u00bb", c.repo, c.hash[:6], t, c.ci.Title)
}

func (w *whatrel) loadRepo(cacheBase string, r *repo) {
	dir := filepath.Join(cacheBase, "repo-"+r.name)
	if st, err := os.Stat(dir); err == nil {
		// have already
		r.git = must(git.PlainOpen(dir))
		if r.refreshedRepo = time.Since(st.ModTime()) > *refreshInterval; r.refreshedRepo {
			// too old, refresh
			log.Println("fetching", r.name)
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
	log.Println("initial clone of", r.name)
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
	log.Println("loading tags on", r.name)
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

func (w *whatrel) loadGoModMap(r *repo) {
	if !r.newTags && !*resetState {
		return
	}
	log.Println("loading go mod map from commits on", r.name)
	// look at tags only to be faster. in theory this may miss some mod names if they were
	// never tagged.
	for _, commit := range r.st.Tags {
		w.loadGoModMapFrom(r, commit)
	}
}

func (w *whatrel) loadGoModMapFrom(r *repo, commit plumbing.Hash) {
	c, err := r.git.CommitObject(commit)
	if err != nil {
		log.Printf("can't find commit %s in %s", commit, r.name)
		return
	}
	f, err := c.File("go.mod")
	if err != nil {
		return
	}
	mod, err := modfile.ParseLax("go.mod", []byte(must(f.Contents())), nil)
	if err != nil {
		log.Println("error parsing go.mod from commit", commit)
		return
	}
	// update gomodmap
	if prev, ok := w.st.GoModMap[mod.Module.Mod.Path]; !ok {
		w.st.GoModMap[mod.Module.Mod.Path] = r.name
		w.st.Modified = true
	} else if prev != r.name {
		panic(fmt.Sprintf("go mod conflict %s vs %s for %s", r.name, prev, mod.Module.Mod.Path))
	}
}

func (w *whatrel) loadCommits(r *repo) {
	if !r.newTags && !*resetState {
		return
	}
	log.Println("loading commits on", r.name)
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
		log.Printf("can't find commit %s in %s", commit, r.name)
		return
	}

	title, _, _ := strings.Cut(c.Message, "\n")

	// get go.mod if present
	deps := make(map[string]plumbing.Hash)
	if f, err := c.File("go.mod"); err == nil {
		if mod, err := modfile.ParseLax("go.mod", []byte(must(f.Contents())), nil); err == nil {
		deps:
			for _, req := range mod.Require {
				p, v := req.Mod.Path, req.Mod.Version
				for _, repl := range mod.Replace {
					if p == repl.Old.Path {
						continue deps
					}
				}
				// find in state
				if depName, ok := w.st.GoModMap[p]; ok {
					depR := w.repos[depName]
					if v == "" {
						log.Printf("version for %s in %s is empty", depName, r.name)
					} else if module.IsPseudoVersion(v) {
						rev := must(module.PseudoVersionRev(v))
						revBytes := must(hex.DecodeString(rev))
						hashes := must(depR.git.Storer.(*filesystem.Storage).HashesWithPrefix(revBytes))
						if len(hashes) == 1 {
							hash := hashes[0]
							if _, ok := w.ignoreDeps[hash]; !ok {
								deps[depName] = hash
							}
						} else {
							log.Printf("version for %s in %s is %s, but found %d hashes", depName, r.name, v, len(hashes))
						}
					} else {
						if hash, ok := depR.st.Tags[v]; ok {
							if _, ok := w.ignoreDeps[hash]; !ok {
								deps[depName] = hash
							}
						} else {
							log.Printf("version for %s in %s is %s, but is unknown tag", depName, r.name, v)
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
					tree := must(c.Tree())
					ent, err := tree.FindEntry(path)
					if err == nil {
						deps[depR.name] = ent.Hash
					} else {
						log.Printf("submodule at %s not found in %s@%s", path, r.name, c.Hash)
					}
				}
			}
		} else {
			log.Printf("error parsing .gitmodules in %s@%s", r.name, c.Hash)
		}
	}

	r.st.Commits[commit] = CommitInfo{
		Title:   title,
		CTime:   c.Committer.When.Unix(),
		Parents: c.ParentHashes,
		Deps:    deps,
	}
	for _, commit := range c.ParentHashes {
		w.loadCommit(r, commit)
	}
	for depName, commit := range deps {
		w.loadCommit(w.repos[depName], commit)
	}
	w.st.Modified = true
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

func (w *whatrel) findCommits(args []string) []commit {
	// form: repo#pr or repo#text or hash prefix
	var out []commit

	for _, arg := range args {
		name, rest, found := strings.Cut(arg, "#")
		if !found {
			if !hashRe.MatchString(arg) {
				log.Fatalln("arg must be: commit | repo#pr | repo#title")
			}
			fullArg := arg
			if len(arg)%2 == 1 {
				arg = arg[:len(arg)-1]
			}
			hashBytes := must(hex.DecodeString(arg))
			for _, r := range w.repos {
				for _, hash := range must(r.git.Storer.(*filesystem.Storage).HashesWithPrefix(hashBytes)) {
					if !strings.HasPrefix(hash.String(), fullArg) {
						continue
					}
					if ci, ok := r.st.Commits[hash]; ok {
						out = append(out, commit{
							repo: r.name,
							ci:   ci,
							hash: hash,
						})
					}
				}
			}
		} else {
			r := w.repos[name]
			if r == nil {
				log.Fatalln("unknown repo:", name)
			}

			var match func(string) bool

			if digitsRe.MatchString(rest) {
				suffix := " (#" + rest + ")"
				match = func(title string) bool { return strings.HasSuffix(title, suffix) }
			} else {
				match = func(title string) bool { return strings.Contains(title, rest) }
			}

			for hash, ci := range r.st.Commits {
				if match(ci.Title) {
					out = append(out, commit{
						repo: r.name,
						ci:   ci,
						hash: hash,
					})
				}
			}
		}
	}

	return out
}

func (w *whatrel) findTags(commit commit) map[string][]string {
	type key struct {
		c plumbing.Hash
		n [12]byte
	}
	cache := make(map[key]bool, 10000)

	var checkCommit func(r *repo, c plumbing.Hash) bool
	checkCommit = func(r *repo, c plumbing.Hash) (retBool bool) {
		if c == commit.hash {
			return true
		}

		k := key{c: c}
		copy(k.n[:], r.name)

		if val, ok := cache[k]; ok {
			return val
		}

		defer func() {
			cache[k] = retBool
		}()

		ci := r.st.Commits[c]
		for _, p := range ci.Parents {
			if checkCommit(r, p) {
				return true
			}
		}
		for depName, depC := range ci.Deps {
			if checkCommit(w.repos[depName], depC) {
				return true
			}
		}
		return false
	}

	out := make(map[string][]string)
	for _, r := range w.repos {
		var tags []string
		for tag, c := range r.st.Tags {
			if checkCommit(r, c) {
				tags = append(tags, tag)
			}
		}
		out[r.name] = tags
	}
	return out
}

func (w *whatrel) printCols(things []string, indent int) {
	const pad = 2

	if w.width == 0 {
		var err error
		w.width, _, err = term.GetSize(1)
		if err != nil {
			w.width = 80
		}
	}

	maxLen := 0
	for _, t := range things {
		maxLen = max(maxLen, len(t))
	}

	if maxLen > 0 {
		cols := max(1, (w.width-indent)/(maxLen+pad))
		slices.Sort(things)
		os.Stdout.WriteString(strings.Repeat(" ", indent))
		for i, t := range things {
			os.Stdout.WriteString(t + strings.Repeat(" ", maxLen-len(t)+pad))
			if (i+1)%cols == 0 && i != len(things)-1 {
				os.Stdout.WriteString("\n" + strings.Repeat(" ", indent))
			}
		}
		os.Stdout.WriteString("\n")
	}
}

func (w *whatrel) printTags(commit commit, allTags map[string][]string) {
	fmt.Printf("%s is in:\n", commit.String())
	for _, r := range w.repos {
		if tags := allTags[r.name]; len(tags) > 0 {
			fmt.Printf("  repo: %s\n", r.name)
			w.printCols(tags, 4)
		}
	}
}

func (w *whatrel) findDeploy(commit commit, allTags map[string][]string, deployState map[string]map[string]map[string]any) {
	normalize := func(s string) string {
		s = strings.TrimPrefix(s, "v")
		s = strings.Replace(s, "+", "_", -1)
		s = strings.Replace(s, "-", "_", -1)
		return s
	}
	fmt.Printf("%s is on:\n", commit.String())
	for _, r := range w.repos {
		if r.cfg.DeployKey != "" {
			tags := allTags[r.name]
			for i, t := range tags {
				tags[i] = normalize(t)
			}
			var found []string
			all := 0
			for tag, things := range deployState[r.cfg.DeployKey] {
				thingsKeys := maps.Keys(things)
				all += len(thingsKeys)
				tag, multi, ok := strings.Cut(tag, "{")
				if ok {
					multi = strings.TrimSuffix(multi, "}")
					var have []string
					for _, m := range strings.Split(multi, ",") {
						if k, v, ok := strings.Cut(m, ":"); ok {
							if slices.Contains(tags, strings.TrimSpace(v)) {
								have = append(have, strings.TrimSpace(k))
							}
						}
					}
					if len(have) == len(strings.Split(multi, ",")) {
						found = append(found, thingsKeys...)
					} else if len(have) > 0 {
						for _, thing := range thingsKeys {
							found = append(found, fmt.Sprintf("%s [%s]", thing, strings.Join(have, ", ")))
						}
					}
				} else if slices.Contains(tags, tag) {
					found = append(found, thingsKeys...)
				}
			}
			if len(found) == all {
				fmt.Printf("  %s: ALL\n", r.cfg.DeployKey)
			} else if len(found) > 0 {
				fmt.Printf("  %s:\n", r.cfg.DeployKey)
				slices.Sort(found)
				w.printCols(found, 4)
			}
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

	w.ignoreDeps = make(map[plumbing.Hash]struct{})
	for _, d := range w.cfg.IgnoreDeps {
		w.ignoreDeps[plumbing.NewHash(d)] = struct{}{}
	}

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
			Repos:         make(map[string]*RepoState),
			GoModMap:      make(map[string]string),
		}
	}

	if *dumpState {
		fmt.Printf("SchemaVersion: %d\n", w.st.SchemaVersion)
		fmt.Printf("GoModMap:\n")
		for k, v := range w.st.GoModMap {
			fmt.Printf("  Mod %-40s -> %s\n", k, v)
		}
		for n, rst := range w.st.Repos {
			fmt.Printf("Repo %s:\n", n)
			for k, v := range rst.Tags {
				fmt.Printf("  Tag %-30s -> %s\n", k, v)
			}
		}
		return
	}

	// write out if modified
	w.st.Modified = false
	defer w.persistState(stateFile)

	w.repos = make(map[string]*repo)
	for name, r := range w.cfg.Repos {
		rst := w.st.Repos[name]
		if rst == nil {
			rst = &RepoState{
				Tags:    make(map[string]plumbing.Hash),
				Commits: make(map[plumbing.Hash]CommitInfo),
			}
			w.st.Repos[name] = rst
		}
		w.repos[name] = &repo{name: name, cfg: r, st: rst}
	}
	for _, r := range w.repos {
		w.loadRepo(cacheBase, r)
	}
	for _, r := range w.repos {
		w.loadTags(r)
	}
	for _, r := range w.repos {
		w.loadGoModMap(r)
	}
	for _, r := range w.repos {
		w.loadCommits(r)
	}

	commits := w.findCommits(flag.Args())

	if *deploy {
		var deployState map[string]map[string]map[string]any
		cmd := exec.Command("sh", "-c", w.cfg.DeployCommand)
		cmd.Stderr = os.Stderr
		stateBytes := must(cmd.Output())
		fatalIfErr(json.Unmarshal(stateBytes, &deployState))
		for _, commit := range commits {
			tags := w.findTags(commit)
			w.findDeploy(commit, tags, deployState)
		}
	} else {
		for _, commit := range commits {
			tags := w.findTags(commit)
			w.printTags(commit, tags)
		}
	}
}
