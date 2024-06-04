package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"gopkg.in/yaml.v3"
)

type (
	Config struct {
		Repos []ConfigRepo
	}

	ConfigRepo struct {
		Name string
		Url  string
	}

	State struct {
		Repos    map[string]*StateRepo
		Modified bool
	}

	StateRepo struct {
		Tags    map[string]plumbing.Hash
		Commits map[plumbing.Hash]CommitInfo
	}

	CommitInfo struct {
		Parents []plumbing.Hash
		Title   string
	}
)

var (
	tagAge          = flag.Int("age", 30, "how many days back to list tags")
	refreshInterval = flag.Duration("refresh", 15*time.Minute, "how often to refresh git repo cache")

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

func refreshRepo(cacheBase string, r ConfigRepo, state *State) *git.Repository {
	dir := filepath.Join(cacheBase, "repo-"+r.Name)
	if st, err := os.Stat(dir); err == nil {
		// have already
		repo := must(git.PlainOpen(dir))

		if time.Since(st.ModTime()) > *refreshInterval {
			// too old, refresh
			repo.Fetch(&git.FetchOptions{
				Tags:  git.AllTags,
				Prune: true,
			})
			loadTags(r, repo, state)
		}
		return repo
	} else if !os.IsNotExist(err) {
		fatalIfErr(err)
	}

	// do initial clone
	repo := must(git.PlainClone(dir, true, &git.CloneOptions{
		URL:    r.Url,
		Mirror: true,
	}))
	loadTags(r, repo, state)
	return repo
}

func loadTags(r ConfigRepo, repo *git.Repository, state *State) {
	if state.Repos == nil {
		state.Repos = make(map[string]*StateRepo)
	}
	repoState := state.Repos[r.Name]
	if repoState == nil {
		repoState = &StateRepo{
			Tags:    make(map[string]plumbing.Hash),
			Commits: make(map[plumbing.Hash]CommitInfo),
		}
		state.Repos[r.Name] = repoState
	}

	must(repo.Tags()).ForEach(func(ref *plumbing.Reference) error {
		tagName := strings.TrimPrefix(ref.Name().String(), "refs/tags/")
		commit := ref.Hash()
		if !versionTagRe.MatchString(tagName) {
			return nil
		}
		if _, ok := repoState.Tags[tagName]; ok {
			return nil
		}
		log.Println("new tag in", r.Name, tagName)
		repoState.Tags[tagName] = commit
		loadCommit(repo, repoState.Commits, commit)
		state.Modified = true
		return nil
	})
}

func loadCommit(repo *git.Repository, cmap map[plumbing.Hash]CommitInfo, commit plumbing.Hash) {
	if _, ok := cmap[commit]; ok {
		return
	}

	if tag, err := repo.TagObject(commit); err == nil {
		commit = tag.Target
	}

	c := must(repo.CommitObject(commit))
	title, _, _ := strings.Cut(c.Message, "\n")
	parents := make([]plumbing.Hash, c.NumParents())
	for i := range parents {
		parents[i] = must(c.Parent(i)).Hash
	}
	cmap[commit] = CommitInfo{
		Title:   title,
		Parents: parents,
	}
	for _, p := range parents {
		loadCommit(repo, cmap, p)
	}
}

func findTags(state *State, arg string) []string {
	// form: repo#pr or repo#text
	name, pr, found := strings.Cut(arg, "#")
	if !found {
		log.Fatalln("arg must be in form repo#pr")
	}

	repoState := state.Repos[name]
	if repoState == nil {
		log.Fatalln("unknown repo", name)
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

	cache := make(map[plumbing.Hash]bool, 10000)

	var checkCommit func(plumbing.Hash) bool
	checkCommit = func(c plumbing.Hash) bool {
		if val, ok := cache[c]; ok {
			return val
		}
		ci := repoState.Commits[c]
		if matchTitle(ci.Title) {
			cache[c] = true
			return true
		}
		if len(ci.Parents) == 0 {
			cache[c] = false
			return false
		}
		val := false
		for _, p := range ci.Parents {
			val = val || checkCommit(p)
		}
		cache[c] = val
		return val
	}

	var out []string
	for tag, commit := range repoState.Tags {
		if checkCommit(commit) {
			out = append(out, tag)
		}
	}
	return out
}

func main() {
	flag.Parse()

	userConfigDir := must(os.UserConfigDir())
	userCacheDir := must(os.UserCacheDir())
	cacheBase := filepath.Join(userCacheDir, "whatrel")

	configFile := filepath.Join(userConfigDir, "whatrel.yaml")
	cfgBytes := must(os.ReadFile(configFile))

	var cfg Config
	fatalIfErr(yaml.Unmarshal(cfgBytes, &cfg))

	stateFile := filepath.Join(cacheBase, "state.gob")
	stateBytes, err := os.ReadFile(stateFile)
	var state State
	if err == nil {
		fatalIfErr(gob.NewDecoder(bytes.NewReader(stateBytes)).Decode(&state))
	} else if !os.IsNotExist(err) {
		fatalIfErr(err)
	}
	state.Modified = false
	defer func() {
		if !state.Modified {
			return
		}
		out := must(os.CreateTemp(cacheBase, "state.gob.tmp"))
		fatalIfErr(gob.NewEncoder(out).Encode(state))
		fatalIfErr(out.Close())
		fatalIfErr(os.Rename(out.Name(), stateFile))
	}()

	for _, r := range cfg.Repos {
		refreshRepo(cacheBase, r, &state)
	}

	for _, arg := range flag.Args() {
		tags := findTags(&state, arg)
		log.Println(arg, "in tags", tags)
	}
}
