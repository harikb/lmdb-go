package main

import (
	"fmt"
	"github.com/harikb/lmdb-go/lmdb"
	flag "github.com/spf13/pflag"
	"log"
	"math/rand"
	"runtime"
)

var loopCount int = 100
var batchCount int = 100
var rewindEnabled bool

func init() {
	flag.IntVarP(&loopCount, "loop-count", "l", 100, "Number of transactions to run")
	flag.IntVarP(&batchCount, "batch-count", "b", 100, "Number of records per trasaction")
	flag.BoolVarP(&rewindEnabled, "enable-rewind", "r", false, "Enable rewind")
	flag.Parse()
}

func main() {

	env, err := lmdb.NewEnv(rewindEnabled)
	if err != nil {
		panic(err)
	}
	defer env.Close()

	// Configure and open the environment.  Most configuration must be done
	// before opening the environment.  The go documentation for each method
	// should indicate if it must be called before calling env.Open()
	err = env.SetMaxDBs(1)
	if err != nil {
		panic(err)
	}
	err = env.SetMapSize(1 << 31)
	if err != nil {
		panic(err)
	}
	err = env.Open("/tmp/lmdb-test/", 0, 0644)
	if err != nil {
		panic(err)
	}

	// In any real application it is important to check for readers that were
	// never closed by their owning process, and for which the owning process
	// has exited.  See the documentation on transactions for more information.
	staleReaders, err := env.ReaderCheck()
	if err != nil {
		panic(err)
	}
	if staleReaders > 0 {
		log.Printf("cleared %d reader slots from dead processes", staleReaders)
	}

	// Open a database handle that will be used for the entire lifetime of this
	// application.  Because the database may not have existed before, and the
	// database may need to be created, we need to get the database handle in
	// an update transacation.
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenRoot(0)
		return err
	})
	if err != nil {
		panic(err)
	}

	// Wrap operations in a struct that can be passed over a channel to a
	// worker goroutine.
	type lmdbop struct {
		op  lmdb.TxnOp
		res chan<- error
	}
	worker := make(chan *lmdbop)
	update := func(op lmdb.TxnOp) error {
		res := make(chan error)
		worker <- &lmdbop{op, res}
		return <-res
	}

	// Start issuing update operations in a goroutine in which we know
	// runtime.LockOSThread can be called and we can safely issue transactions.
	go func() {
		runtime.LockOSThread()
		defer runtime.LockOSThread()

		// Perform each operation as we get to it.  Because this goroutine is
		// already locked to a thread, env.UpdateLocked is called to avoid
		// premature unlocking of the goroutine from its thread.
		for op := range worker {
			op.res <- env.UpdateLocked(op.op)
		}
	}()

	// In another goroutine, where we cannot determine if we are locked to a
	// thread already.
	type LoopKeys struct {
		i int
		j int
	}
	randomizedKeys := map[string]LoopKeys{}
	for i := 0; i < loopCount; i++ {
		err = update(func(txn *lmdb.Txn) (err error) {
			// This function will execute safely in the worker goroutine, which is
			// locked to its thread.
			for j := 0; j < batchCount; j++ {
				key := fmt.Sprintf("hello-%10d-%10d", rand.Uint32(), rand.Uint32())
				randomizedKeys[key] = LoopKeys{i, j}
				err = txn.Put(dbi, []byte(key),
					[]byte(fmt.Sprintf("%0500d%0500d", i, j)), 0)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	log.Printf("Updated %d entries", loopCount*batchCount)

	// The database referenced by our DBI handle is now ready for the
	// application to use.  Here the application just opens a readonly
	// transaction and reads the data stored in the "hello" key and prints its
	// value to the application's standard output.
	err = env.View(func(txn *lmdb.Txn) (err error) {
		for randKey, ix := range randomizedKeys {
			expected := fmt.Sprintf("%0500d%0500d", ix.i, ix.j)
			v, err := txn.Get(dbi, []byte(randKey))
			if err != nil {
				return err
			}
			if string(v) != expected {
				log.Println(string(v))
			}
		}
		log.Printf("Checked %d entries", loopCount*batchCount)
		return nil
	})
	if err != nil {
		panic(err)
	}
}

