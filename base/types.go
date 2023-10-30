package base

import (
	"config"
	"fmt"
	"sync/atomic"
	"time"
)

/* To properly define message */
type Message struct {
	Command int
	Key     string
	Data    string // for client

	SrcID   int     // for inter-node
	ObjData *Object // for inter-node
}

/* Versioning information */
type Context struct {
	v_clk []int
}

func (c *Context) Copy() *Context {
	ret := new(Context)
	ret.v_clk = make([]int, len(c.v_clk))
	copy(ret.v_clk, c.v_clk)
	return ret
}

type Object struct {
	context   *Context
	data      string
	isReplica bool
}

func (o *Object) GetData() string {
	return o.data
}

func (o *Object) IsReplica() bool {
	return o.isReplica
}

func (o *Object) Copy() Object {
	return Object{context: o.context.Copy(), data: o.data, isReplica: o.isReplica}
}

func (o *Object) ToString() string {
	return fmt.Sprintf("context=%v, data=%s, isReplica=%v", o.context, o.data, o.isReplica)
}

type Node struct {
	id        int
	v_clk     []int
	channels  map[int](chan Message)
	rcv_ch    chan Message
	client_ch chan Message //communicates with "frontend" client
	tokens    []*Token
	data      map[string]*Object           // key-value data store
	backup    map[int](map[string]*Object) // backup of key-value data stores
	close_ch  chan struct{}                //to close go channels properly

	awaitAck    map[int](*atomic.Bool) // flags to check on timeout routines
	tokenStruct BST
	aliveSince  time.Time

	//state machine for Get()
	numReads       int
	timeoutCounter int
}

func (n *Node) GetTokens() []*Token {
	return n.tokens
}
func (n *Node) GetData(key string) *Object {
	obj, exists := n.data[key]
	if !exists {
		return &Object{}
	}
	return obj
}

func (n *Node) GetID() int {
	return n.id
}

func (n *Node) GetTokenStruct() BST {
	return n.tokenStruct
}

func (n *Node) GetAliveSince() time.Time {
	return n.aliveSince
}

func (n *Node) SetAliveSince(alive time.Time) {
	n.aliveSince = alive
}

type Token struct {
	id          int
	phy_id      int
	range_start string
	range_end   string
}

func (t *Token) GetID() int {
	return t.id
}

func (t *Token) GetStartRange() string {
	return t.range_start
}

func (t *Token) GetEndRange() string {
	return t.range_end
}

type TreeNode struct {
	Token *Token
	Left  *TreeNode
	Right *TreeNode
}

type BST struct {
	Root *TreeNode
}

// Insert a new Token into the BST.
func (bst *BST) Insert(token *Token) {
	bst.Root = bst.insertTok(bst.Root, token)
}

func (bst *BST) insertTok(root *TreeNode, token *Token) *TreeNode {
	if root == nil {
		return &TreeNode{Token: token}
	}

	if token.range_start < root.Token.range_start {
		root.Left = bst.insertTok(root.Left, token)
	} else {
		root.Right = bst.insertTok(root.Right, token)
	}

	return root
}

// Search for a Token whose range includes the given value.
func (bst *BST) Search(value string) *TreeNode {
	return bst.searchTok(bst.Root, value)
}

func (bst *BST) searchTok(root *TreeNode, value string) *TreeNode {
	if config.DEBUG_LEVEL >= 3 {
		fmt.Printf("token %d, range start = %s, range end = %s, value = %s\n",
			root.Token.id, root.Token.range_start, root.Token.range_end, value)
	}

	if root == nil {
		return nil
	}

	if hashInRange(value, root.Token.GetStartRange(), root.Token.GetEndRange()) {
		return root
	}

	if hashInRange(value, value, root.Token.GetStartRange()) {
		return bst.searchTok(root.Left, value)
	}

	return bst.searchTok(root.Right, value)
}

func (bst *BST) getNext(node *TreeNode) *TreeNode {
	// The next node is basically the leftmost node in the right subtree.
	if node.Right != nil {
		return bst.leftMostNode(node.Right)
	}

	// If no right subtree, then it will be the nearest ancestor for which
	// the given node would be in the left subtree.
	var successor *TreeNode
	ancestor := bst.Root
	for ancestor != node {
		if node.Token.range_start < ancestor.Token.range_start {
			successor = ancestor
			ancestor = ancestor.Left
		} else {
			ancestor = ancestor.Right
		}
	}

	// If successor is nil, return the leftmost node in the tree.
	if successor == nil {
		return bst.leftMostNode(bst.Root)
	}
	return successor
}

func (bst *BST) leftMostNode(node *TreeNode) *TreeNode {
	current := node
	for current.Left != nil {
		current = current.Left
	}
	return current
}

func (bst *BST) PrintBST() {
	fmt.Printf("%v\n", bst.Root.Token)
	node := bst.getNext(bst.Root)
	for node != bst.Root {
		fmt.Printf("%v\n", node.Token)
		node = bst.getNext(node)
	}
}
