package kvstore

import (
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"math"
)

// # Stores :
//                k_   ->   peptide kmers => [gg_key, ff_key, pp_key, oo_key]
//                kk_  ->   k_ combination store
//                g_   ->   gene ontology
//                gg_  ->   g_ combination store
//                f_   ->   function (uniprot)
//                ff_  ->   f_ combination store
//                p_   ->   pathway
//                pp_  ->   p_ combination store
//                o_   ->   taxonomic lineage
//                oo_  ->   o_ combination store
//
//
//  Each store uses a combination pattern to reduce its size (flyweight design pattern)
//  '.' prefix are for real keys and '_' prefix for combination keys
//  Combination keys are SHA1SUM of the content
//  Example :
//              '.MSAVALPRVSG' => '_213a326b89b'
//              '_213a326b89b' => '[g_key,f_key,p_key,o_key]'
//

type KVStores struct {
	K_batch         *K_
	KK_batch        *H_
	G_batch         *G_
	GG_batch        *H_
	F_batch         *F_
	FF_batch        *H_
	P_batch         *P_
	PP_batch        *H_
	O_batch         *O_
	OO_batch        *H_
	N_batch         *N_
	NN_batch        *H_
}

func KVStoresNew (dbPath string, nbOfThreads int) *KVStores {

	var kvStores KVStores

	k_opts := badger.DefaultOptions
	k_opts.Dir = dbPath+"/k_store"
	k_opts.ValueDir = dbPath+"/k_store"
	k_opts.ValueLogLoadingMode = options.FileIO
	k_opts.TableLoadingMode = options.MemoryMap
	//k_opts.SyncWrites = false
	k_opts.NumVersionsToKeep = math.MaxUint32

	kk_opts := badger.DefaultOptions
	kk_opts.Dir = dbPath+"/kk_store"
	kk_opts.ValueDir = dbPath+"/kk_store"
	kk_opts.ValueLogLoadingMode = options.FileIO
	kk_opts.TableLoadingMode = options.MemoryMap
	//kk_opts.SyncWrites = false
	kk_opts.NumVersionsToKeep = 1

	g_opts := badger.DefaultOptions
	g_opts.Dir = dbPath+"/g_store"
	g_opts.ValueDir = dbPath+"/g_store"
	g_opts.ValueLogLoadingMode = options.FileIO
	g_opts.TableLoadingMode = options.MemoryMap
	//g_opts.SyncWrites = false
	g_opts.NumVersionsToKeep = 1

	gg_opts := badger.DefaultOptions
	gg_opts.Dir = dbPath+"/gg_store"
	gg_opts.ValueDir = dbPath+"/gg_store"
	gg_opts.ValueLogLoadingMode = options.FileIO
	gg_opts.TableLoadingMode = options.MemoryMap
	//gg_opts.SyncWrites = false
	gg_opts.NumVersionsToKeep = 1

	f_opts := badger.DefaultOptions
	f_opts.Dir = dbPath+"/f_store"
	f_opts.ValueDir = dbPath+"/f_store"
	f_opts.ValueLogLoadingMode = options.FileIO
	f_opts.TableLoadingMode = options.MemoryMap
	//f_opts.SyncWrites = false
	f_opts.NumVersionsToKeep = 1

	ff_opts := badger.DefaultOptions
	ff_opts.Dir = dbPath+"/ff_store"
	ff_opts.ValueDir = dbPath+"/ff_store"
	ff_opts.ValueLogLoadingMode = options.FileIO
	ff_opts.TableLoadingMode = options.MemoryMap
	//ff_opts.SyncWrites = false
	ff_opts.NumVersionsToKeep = 1

	p_opts := badger.DefaultOptions
	p_opts.Dir = dbPath+"/p_store"
	p_opts.ValueDir = dbPath+"/p_store"
	p_opts.ValueLogLoadingMode = options.FileIO
	p_opts.TableLoadingMode = options.MemoryMap
	//p_opts.SyncWrites = false
	p_opts.NumVersionsToKeep = 1

	pp_opts := badger.DefaultOptions
	pp_opts.Dir = dbPath+"/pp_store"
	pp_opts.ValueDir = dbPath+"/pp_store"
	pp_opts.ValueLogLoadingMode = options.FileIO
	pp_opts.TableLoadingMode = options.MemoryMap
	//pp_opts.SyncWrites = false
	pp_opts.NumVersionsToKeep = 1

	o_opts := badger.DefaultOptions
	o_opts.Dir = dbPath+"/o_store"
	o_opts.ValueDir = dbPath+"/o_store"
	o_opts.ValueLogLoadingMode = options.FileIO
	o_opts.TableLoadingMode = options.MemoryMap
	//o_opts.SyncWrites = false
	o_opts.NumVersionsToKeep = 1

	oo_opts := badger.DefaultOptions
	oo_opts.Dir = dbPath+"/oo_store"
	oo_opts.ValueDir = dbPath+"/oo_store"
	oo_opts.ValueLogLoadingMode = options.FileIO
	oo_opts.TableLoadingMode = options.MemoryMap
	//oo_opts.SyncWrites = false
	oo_opts.NumVersionsToKeep = 1

	n_opts := badger.DefaultOptions
	n_opts.Dir = dbPath+"/n_store"
	n_opts.ValueDir = dbPath+"/n_store"
	n_opts.ValueLogLoadingMode = options.FileIO
	n_opts.TableLoadingMode = options.MemoryMap
	//n_opts.SyncWrites = false
	n_opts.NumVersionsToKeep = 1

	nn_opts := badger.DefaultOptions
	nn_opts.Dir = dbPath+"/nn_store"
	nn_opts.ValueDir = dbPath+"/nn_store"
	nn_opts.ValueLogLoadingMode = options.FileIO
	nn_opts.TableLoadingMode = options.MemoryMap
	//nn_opts.SyncWrites = false
	nn_opts.NumVersionsToKeep = 1


	kvStores.K_batch = K_New(k_opts, 1000, nbOfThreads)
	kvStores.KK_batch = H_New(kk_opts, 1000, nbOfThreads)
	kvStores.G_batch = G_New(g_opts, 1000, nbOfThreads)
	kvStores.GG_batch = H_New(gg_opts, 1000, nbOfThreads)
	kvStores.F_batch = F_New(f_opts, 1000, nbOfThreads)
	kvStores.FF_batch = H_New(ff_opts, 1000, nbOfThreads)
	kvStores.P_batch = P_New(p_opts, 1000, nbOfThreads)
	kvStores.PP_batch = H_New(pp_opts, 1000, nbOfThreads)
	kvStores.O_batch = O_New(o_opts, 1000, nbOfThreads)
	kvStores.OO_batch = H_New(oo_opts, 1000, nbOfThreads)
	kvStores.N_batch = N_New(n_opts, 1000, nbOfThreads)
	kvStores.NN_batch = H_New(nn_opts, 1000, nbOfThreads)

	return &kvStores

}

func (kvStores *KVStores) Flush () {
	// Last DB flushes
	kvStores.K_batch.Flush()
	kvStores.KK_batch.Flush()
	kvStores.G_batch.Flush()
	kvStores.GG_batch.Flush()
	kvStores.F_batch.Flush()
	kvStores.FF_batch.Flush()
	kvStores.P_batch.Flush()
	kvStores.PP_batch.Flush()
	kvStores.O_batch.Flush()
	kvStores.OO_batch.Flush()
	kvStores.N_batch.Flush()
	kvStores.NN_batch.Flush()
}


func (kvStores *KVStores) Close () {
	// Last DB flushes
	kvStores.K_batch.Close()
	kvStores.KK_batch.Close()
	kvStores.G_batch.Close()
	kvStores.GG_batch.Close()
	kvStores.F_batch.Close()
	kvStores.FF_batch.Close()
	kvStores.P_batch.Close()
	kvStores.PP_batch.Close()
	kvStores.O_batch.Close()
	kvStores.OO_batch.Close()
	kvStores.N_batch.Close()
	kvStores.NN_batch.Close()
}
