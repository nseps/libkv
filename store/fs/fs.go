package fs

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/rjeczalik/notify"
	"github.com/thegrumpylion/libkv"
	"github.com/thegrumpylion/libkv/store"
)

var (
	ErrMultipleEndpointsUnsupported = errors.New("FS supports one endpoint and should be a file path")
	ErrFSDirDoesNotExist            = errors.New("FS directory does not exist")
	ErrEntpointNotADirectory        = errors.New("FS endpoint is not a drectory")
	ErrDoubleDotPathNotAllowed      = errors.New(".. is not allowed as part of the key")
	ErrMkDir                        = errors.New("FS failed to mkdir")
)

// FS Filesystem store
type FS struct {
	sync.Mutex
	locks map[string]*sync.RWMutex
	path  string
}

// Register registers fs to libkv
func Register() {
	libkv.AddStore(store.FS, New)
}

// New creates a Filesystem store
func New(endpoints []string, opts *store.Config) (store.Store, error) {

	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	path := filepath.Clean(endpoints[0])
	info, err := os.Stat(path)

	if os.IsNotExist(err) {
		err := os.MkdirAll(path, 0755)
		if err != nil {
			return nil, err
		}
	} else {
		if !info.IsDir() {
			return nil, ErrEntpointNotADirectory
		}
	}

	return &FS{
		locks: map[string]*sync.RWMutex{},
		path:  filepath.Clean(path),
	}, nil
}

func (s *FS) getLock(key string) *sync.RWMutex {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.locks[key]; !ok {
		s.locks[key] = &sync.RWMutex{}
	}
	return s.locks[key]
}

func (s *FS) removeLock(key string) {
	s.Lock()
	defer s.Unlock()
	delete(s.locks, key)
}

// Lock Filesystem implementation of Locker
type Lock struct {
}

// Lock Filesystem
func (l *Lock) Lock(stopCh chan struct{}) (<-chan struct{}, error) {
	return nil, nil
}

// Unlock Filesystem
func (l *Lock) Unlock() error {
	return nil
}

func (s *FS) exist(p string) (bool, string, os.FileInfo, error) {
	info, err := os.Stat(p)
	if os.IsNotExist(err) {
		return false, "", nil, nil
	}
	// catch other errors
	if err != nil {
		return false, "", nil, err
	}
	if info.IsDir() {
		p = dirValue(p)
		info, err = os.Stat(p)
		if os.IsNotExist(err) {
			return false, p, nil, nil
		}
	}
	return true, p, info, nil
}

func (s *FS) get(p string) ([]byte, error) {
	f, lk, err := openRdLk(p)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	err = closeUnlk(f, lk)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *FS) put(p string, v []byte, m os.FileMode) (os.FileInfo, error) {
	d := filepath.Dir(p)

	if d != s.path {
		err := os.MkdirAll(d, 0755)
		if err != nil {
			if strings.HasSuffix(err.Error(), ": not a directory") {
				fl := strings.TrimSuffix(strings.TrimPrefix(err.Error(), "mkdir "), ": not a directory")
				err = os.Rename(fl, dirValue(fl))
				if err != nil {
					return nil, err
				}
				err := os.MkdirAll(d, 0755)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}
	}

	inf, err := os.Stat(p)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if inf != nil && inf.IsDir() {
		p = dirValue(p)
	}

	f, lk, err := openWrLk(p)
	if err != nil {
		return nil, err
	}

	ftmp, err := ioutil.TempFile(d, filepath.Base(p))
	if err != nil {
		return nil, err
	}

	if _, err = ftmp.Write(v); err != nil {
		return nil, err
	}

	if err := ftmp.Chmod(m); err != nil {
		return nil, err
	}
	ftmp.Close()

	err = os.Rename(ftmp.Name(), p)
	if err != nil {
		return nil, err
	}

	inf, err = os.Stat(p)
	if err != nil {
		return nil, err
	}

	return inf, closeUnlk(f, lk)
}

func (s *FS) delete(p string, comp uint64) error {
	exists, path, inf, err := s.exist(p)
	if err != nil {
		return err
	}

	if !exists && (path == "" || comp != 0) {
		return store.ErrKeyNotFound
	}

	if !exists && path != "" {
		// remove empty dir
		err := os.Remove(path)
		if err != nil {
			return store.ErrKeyNotFound
		}
		return nil
	}

	if comp != 0 {
		if uint64(inf.ModTime().Unix()) != comp {
			return store.ErrKeyModified
		}
	}

	f, lk, err := openWrLk(path)
	if err != nil {
		return err
	}

	if err = os.Remove(path); err != nil {
		return err
	}

	// remove empty directories backwards
	for {
		err = os.Remove(p)
		if err != nil {
			break
		}
		p = filepath.Dir(p)
		if p == s.path {
			break
		}
	}

	return closeUnlk(f, lk)
}

func (s *FS) list() {

}

func (s *FS) Get(key string, opts *store.ReadOptions) (*store.KVPair, error) {
	if !isValid(key) {
		return nil, ErrDoubleDotPathNotAllowed
	}
	k := normalize(key)

	p := filepath.Join(s.path, k)

	exists, p, inf, err := s.exist(p)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrKeyNotFound
	}

	l := s.getLock(k)
	l.RLock()
	defer l.RUnlock()

	data, err := s.get(p)
	if err != nil {
		return nil, err
	}

	return &store.KVPair{
		Key:       key,
		Value:     data,
		LastIndex: uint64(inf.ModTime().Unix()),
	}, nil
}

func (s *FS) Put(key string, value []byte, opts *store.WriteOptions) error {
	if !isValid(key) {
		return ErrDoubleDotPathNotAllowed
	}
	key = normalize(key)

	m := os.FileMode(0666)
	if opts != nil {
		if opts.Mode != 0 {
			m = opts.Mode
		}
	}

	l := s.getLock(key)
	l.Lock()
	defer l.Unlock()

	_, err := s.put(filepath.Join(s.path, key), value, m)
	if err != nil {
		return err
	}

	return nil
}

func (s *FS) Delete(key string) error {
	if !isValid(key) {
		return ErrDoubleDotPathNotAllowed
	}
	key = normalize(key)

	p := filepath.Join(s.path, key)

	l := s.getLock(key)
	l.Lock()
	defer l.Unlock()

	if err := s.delete(p, 0); err != nil {
		return err
	}

	s.removeLock(key)

	return nil
}

func (s *FS) Exists(key string, opts *store.ReadOptions) (bool, error) {
	if !isValid(key) {
		return false, ErrDoubleDotPathNotAllowed
	}
	key = normalize(key)

	p := filepath.Join(s.path, key)

	ex, _, _, err := s.exist(p)

	return ex, err
}

func (s *FS) Watch(key string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan *store.KVPair, error) {
	if !isValid(key) {
		return nil, ErrDoubleDotPathNotAllowed
	}
	p := filepath.Join(s.path, key)

	chn := make(chan *store.KVPair)

	exists, p, inf, err := s.exist(p)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrKeyNotFound
	}

	value, err := s.get(p)
	if err != nil {
		return nil, err
	}
	pair := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: uint64(inf.ModTime().Unix()),
	}

	dir := filepath.Dir(p)

	c := make(chan notify.EventInfo, 1)

	if err := notify.Watch(dir, c, notify.All); err != nil {
		return nil, err
	}

	go func() {
		defer close(chn)
		defer notify.Stop(c)
		chn <- pair
		for {
			select {
			case <-stopCh:
				return
			case ev := <-c:
				if ev.Path() != p {
					continue
				}
				var data []byte
				var idx uint64
				if ev.Event() == notify.Create || ev.Event() == notify.Write {
					data, err = ioutil.ReadFile(ev.Path())
					if err != nil {
						return
					}
					inf, err := os.Stat(p)
					if err != nil {
						return
					}
					idx = uint64(inf.ModTime().Unix())
				}
				chn <- &store.KVPair{
					Key:       key,
					Value:     data,
					LastIndex: idx,
				}
			}
		}
	}()

	return chn, nil
}

func (s *FS) WatchTree(directory string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan []*store.KVPair, error) {
	if !isValid(directory) {
		return nil, ErrDoubleDotPathNotAllowed
	}
	p := filepath.Join(s.path, normalize(directory))

	chn := make(chan []*store.KVPair)

	inf, err := os.Stat(p)
	if os.IsNotExist(err) {
		return nil, store.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	if !inf.IsDir() {
		return nil, ErrFSDirDoesNotExist
	}

	pairs, err := s.List(directory, opts)
	if err != nil {
		return nil, err
	}

	c := make(chan notify.EventInfo, 1)

	if err := notify.Watch(filepath.Join(p, "...."), c, notify.All); err != nil {
		return nil, err
	}

	go func() {
		defer close(chn)
		defer notify.Stop(c)
		chn <- pairs
		for {
			select {
			case <-stopCh:
				return
			case ev := <-c:
				var data []byte
				var idx uint64
				if ev.Event() == notify.Create || ev.Event() == notify.Write {
					data, err = ioutil.ReadFile(ev.Path())
					if err != nil {
						return
					}
					inf, err := os.Stat(p)
					if err != nil {
						return
					}
					idx = uint64(inf.ModTime().Unix())
				}
				chn <- []*store.KVPair{{
					Key:       strings.TrimPrefix(ev.Path(), s.path),
					Value:     data,
					LastIndex: idx,
				}}
			}
		}
	}()

	return chn, nil
}

func (s *FS) NewLock(key string, opts *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

func (s *FS) List(directory string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	if !isValid(directory) {
		return nil, ErrDoubleDotPathNotAllowed
	}

	d := normalize(directory)
	p := filepath.Clean(s.path + d)
	out := []*store.KVPair{}

	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil, store.ErrKeyNotFound
	}

	err := filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
		if path == p {
			return nil
		}
		if info.Mode().IsRegular() {

			name := info.Name()
			if n, ok := isDirValue(name); ok {
				name = n
			}
			key := d + "/" + name

			l := s.getLock(key)
			l.RLock()
			defer l.RUnlock()

			f, lk, err := openRdLk(path)
			if err != nil {
				return err
			}

			data, err := ioutil.ReadAll(f)
			if err != nil {
				return err
			}

			if err := closeUnlk(f, lk); err != nil {
				return err
			}

			out = append(out, &store.KVPair{
				Key:       key,
				Value:     data,
				LastIndex: uint64(info.ModTime().Unix()),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (s *FS) DeleteTree(directory string) error {
	if !isValid(directory) {
		return ErrDoubleDotPathNotAllowed
	}

	d := normalize(directory)
	p := filepath.Join(s.path, d)
	val := dirValue(p)

	s.Lock()
	defer s.Unlock()

	for _, l := range s.locks {
		l.Lock()
	}

	err := os.RemoveAll(p)
	if err != nil {
		return err
	}

	err = os.RemoveAll(val)
	if err != nil {
		return err
	}

	s.locks = map[string]*sync.RWMutex{}

	return nil
}

func (s *FS) AtomicPut(key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	if !isValid(key) {
		return false, nil, ErrDoubleDotPathNotAllowed
	}

	key = normalize(key)
	p := filepath.Join(s.path, key)

	exists, path, inf, err := s.exist(p)
	if err != nil {
		return false, nil, err
	}

	if previous == nil {
		if exists {
			return false, nil, store.ErrKeyExists
		}
		path = p
	} else {
		if uint64(inf.ModTime().Unix()) != previous.LastIndex {
			return false, nil, store.ErrKeyModified
		}
	}

	m := os.FileMode(0666)
	if opts != nil {
		if opts.Mode != 0 {
			m = opts.Mode
		}
	}

	l := s.getLock(key)
	l.Lock()
	defer l.Unlock()

	inf, err = s.put(path, value, m)
	if err != nil {
		return false, nil, err
	}

	updated := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: uint64(inf.ModTime().Unix()),
	}

	return true, updated, nil
}

func (s *FS) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if previous == nil {
		return false, store.ErrPreviousNotSpecified
	}

	if !isValid(key) {
		return false, ErrDoubleDotPathNotAllowed
	}

	p := filepath.Join(s.path, normalize(key))

	l := s.getLock(key)
	l.Lock()
	defer l.Unlock()

	err := s.delete(p, previous.LastIndex)
	if err != nil {
		return false, err
	}

	s.removeLock(key)

	return true, nil
}

func (s *FS) Close() {
}

// openRdLk opens file at "path" and tries to acuire a read lock on it
func openRdLk(path string) (*os.File, *unix.Flock_t, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	lk := &unix.Flock_t{
		Type: unix.F_RDLCK,
	}
	err = unix.FcntlFlock(f.Fd(), unix.F_SETLKW, lk)
	if err != nil {
		return nil, nil, err
	}
	return f, lk, nil
}

// openWrLk opens file at "path" and tries to acuire a write lock on it
func openWrLk(path string) (*os.File, *unix.Flock_t, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}
	lk := &unix.Flock_t{
		Type: unix.F_WRLCK,
	}
	err = unix.FcntlFlock(f.Fd(), unix.F_SETLKW, lk)
	if err != nil {
		return nil, nil, err
	}
	return f, lk, nil
}

// closeUnlk closes the file and releases the lock
func closeUnlk(f *os.File, lk *unix.Flock_t) error {
	lk.Type = unix.F_UNLCK
	err := unix.FcntlFlock(f.Fd(), unix.F_SETLKW, lk)
	if err != nil {
		return err
	}
	return f.Close()
}

func isValid(path string) bool {
	arr := strings.Split(path, "/")
	for _, s := range arr {
		if s == ".." {
			return false
		}
	}
	return true
}

func normalize(key string) string {
	if key[0] != '/' {
		key = "/" + key
	}
	return strings.TrimSuffix(key, "/")
}

const dirValuePfx = "."
const dirValueSfx = ""

func dirValue(path string) string {
	return filepath.Join(filepath.Dir(path), dirValuePfx+filepath.Base(path)+dirValueSfx)
}

func isDirValue(name string) (string, bool) {
	if strings.HasPrefix(name, dirValuePfx) && strings.HasSuffix(name, dirValueSfx) {
		name = strings.TrimPrefix(name, dirValuePfx)
		name = strings.TrimSuffix(name, dirValueSfx)
		return name, true
	}
	return "", false
}
