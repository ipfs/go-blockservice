package bstest

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap"
	"github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-blockservice"
	. "github.com/ipfs/go-blockservice"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
	p2ptestutil "github.com/libp2p/go-libp2p-netutil"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	u "github.com/ipfs/go-ipfs-util"
)

func newObject(data []byte) blocks.Block {
	return blocks.NewBlock(data)
}

func TestBlocks(t *testing.T) {
	bstore := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bs := New(bstore, offline.Exchange(bstore))
	defer bs.Close()

	o := newObject([]byte("beep boop"))
	h := cid.NewCidV0(u.Hash([]byte("beep boop")))
	if !o.Cid().Equals(h) {
		t.Error("Block key and data multihash key not equal")
	}

	err := bs.AddBlock(o)
	if err != nil {
		t.Error("failed to add block to BlockService", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	b2, err := bs.GetBlock(ctx, o.Cid())
	if err != nil {
		t.Error("failed to retrieve block from BlockService", err)
		return
	}

	if !o.Cid().Equals(b2.Cid()) {
		t.Error("Block keys not equal.")
	}

	if !bytes.Equal(o.RawData(), b2.RawData()) {
		t.Error("Block data is not equal.")
	}
}

func makeObjects(n int) []blocks.Block {
	var out []blocks.Block
	for i := 0; i < n; i++ {
		out = append(out, newObject([]byte(fmt.Sprintf("object %d", i))))
	}
	return out
}

func TestGetBlocksSequential(t *testing.T) {
	var servs = Mocks(4)
	for _, s := range servs {
		defer s.Close()
	}
	objs := makeObjects(50)

	var cids []cid.Cid
	for _, o := range objs {
		cids = append(cids, o.Cid())
		err := servs[0].AddBlock(o)
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Log("one instance at a time, get blocks concurrently")

	for i := 1; i < len(servs); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
		defer cancel()
		out := servs[i].GetBlocks(ctx, cids)
		gotten := make(map[string]blocks.Block)
		for blk := range out {
			if _, ok := gotten[blk.Cid().KeyString()]; ok {
				t.Fatal("Got duplicate block!")
			}
			gotten[blk.Cid().KeyString()] = blk
		}
		if len(gotten) != len(objs) {
			t.Fatalf("Didnt get enough blocks back: %d/%d", len(gotten), len(objs))
		}
	}
}

func TestDoubleWrite(t *testing.T) {
	ctx := context.Background()
	mn := mocknet.New(ctx)
	p, err := p2ptestutil.RandTestBogusIdentity()
	if err != nil {
		t.Fatal("error setting up identity")
	}
	host, err := mn.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		t.Fatal("error creating host")
	}
	mr := mockrouting.NewServer()
	routing := mr.ClientWithDatastore(context.TODO(), p, ds.NewMapDatastore())
	adapter := network.NewFromIpfsHost(host, routing)
	received := make(chan struct{}, 2)
	bstore := &mockstore{received: received}
	bs := bitswap.New(ctx, adapter, bstore)
	blockService := blockservice.New(bstore, bs)
	blk := newObject([]byte("beep boop"))
	blockService.AddBlock(blk)
	timer := time.NewTimer(2 * time.Second)
	alreadyWritten := false
	for {
		select {
		case <-timer.C:
			return
		case <-received:
			if alreadyWritten {
				t.Fatal("put called twice on blockstore")
			}
			alreadyWritten = true
		}
	}
}

type mockstore struct {
	received chan struct{}
}

func (ms *mockstore) DeleteBlock(_ cid.Cid) error {
	panic("not implemented")
}

func (ms *mockstore) Has(_ cid.Cid) (bool, error) {
	return false, nil
}

func (ms *mockstore) Get(_ cid.Cid) (blocks.Block, error) {
	panic("not implemented")
}

// GetSize returns the CIDs mapped BlockSize
func (ms *mockstore) GetSize(_ cid.Cid) (int, error) {
	panic("not implemented")
}

// Put puts a given block to the underlying datastore
func (ms *mockstore) Put(_ blocks.Block) error {
	ms.received <- struct{}{}
	return nil
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (ms *mockstore) PutMany(_ []blocks.Block) error {
	ms.received <- struct{}{}
	return nil
}

// AllKeysChan returns a channel from which
// the CIDs in the Blockstore can be read. It should respect
// the given context, closing the channel if it becomes Done.
func (ms *mockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	panic("not implemented")
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (ms *mockstore) HashOnRead(enabled bool) {
	panic("not implemented")
}
