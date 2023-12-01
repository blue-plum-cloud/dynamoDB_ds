package base

import (
	"config"
	"constants"
	"fmt"
	"sync"
	"sync/atomic"
)

type Client struct {
	Id         int
	Close      chan struct{}
	Client_ch  chan Message
	AwaitUids  map[int](*atomic.Bool)
	NewestRead string
}

type Message struct {
	JobId   int
	Command int
	Key     string
	Data    string // for client
	Wcount  int

	SrcID   int     // for inter-node
	ObjData *Object // for inter-node

	HandoffToken *Token // for inter-node

	Client_Ch chan Message
}

func (m *Message) ToString(targetID int) string {
	return fmt.Sprintf("%d->%d %s \t\t JobId=%d, Key=%s, Data=%s, ObjData=(%s)",
		m.SrcID, targetID, constants.GetConstantString(m.Command), m.JobId, m.Key, m.Data, m.ObjData.ToString())
}

func (m *Message) Copy() Message {
	return Message{JobId: m.JobId, Command: m.Command, Key: m.Key, Data: m.Data, Wcount: m.Wcount, SrcID: m.SrcID, ObjData: m.ObjData.Copy(), Client_Ch: m.Client_Ch}
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

func (o *Object) Copy() *Object {
	return &Object{context: o.context.Copy(), data: o.data, isReplica: o.isReplica}
}

func (o *Object) ToString() string {
	if o == nil {
		return ""
	}
	return fmt.Sprintf("context=%v, data=%s, isReplica=%v", o.context, o.data, o.isReplica)
}

type Node struct {
	id       int
	v_clk    []int
	channels map[int](chan Message)
	rcv_ch   chan Message
	tokens   []*Token
	data     map[string]*Object           // key-value data store
	backup   map[int](map[string]*Object) // backup of key-value data stores
	close_ch chan struct{}                //to close go channels properly

	awaitAck     map[int](*atomic.Bool) // flags to check on timeout routines
	tokenStruct  BST
	prefList     map[*Token][]*TreeNode
	handOffQueue []*Token

	// Locking for concurrent rep
	mutex       sync.Mutex
	numReads    map[int]int
	readTimeout chan int
}

func (n *Node) GetPrefList() map[*Token][]*TreeNode {
	return n.prefList
}

func (n *Node) GetChannel() chan Message {
	return n.rcv_ch
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

type Token struct {
	id          int
	phy_id      int
	range_start string
	range_end   string
}

func (t *Token) GetID() int {
	return t.id
}

func (t *Token) GetPID() int {
	return t.phy_id
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
func (bst *BST) Search(value string, c *config.Config) *TreeNode {
	return bst.searchTok(bst.Root, value, c)
}

func (bst *BST) searchTok(root *TreeNode, value string, c *config.Config) *TreeNode {
	if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
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
		return bst.searchTok(root.Left, value, c)
	}

	return bst.searchTok(root.Right, value, c)
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
	for ancestor != nil {
		if node.Token.range_start < ancestor.Token.range_start {
			successor = ancestor
			ancestor = ancestor.Left
		} else if node.Token.range_start > ancestor.Token.range_start {
			ancestor = ancestor.Right
		} else {
			break
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

func (bst *BST) getPrev(node *TreeNode) *TreeNode {
	// If the left subtree exists, return the rightmost node of the left subtree
	if node.Left != nil {
		return bst.rightMostNode(node.Left)
	}

	// If no left subtree, find the nearest ancestor for which
	// the given node would be in the right subtree
	var predecessor *TreeNode
	ancestor := bst.Root
	for ancestor != node {
		if node.Token.range_start > ancestor.Token.range_start {
			predecessor = ancestor
			ancestor = ancestor.Right
		} else {
			ancestor = ancestor.Left
		}
	}

	// If predecessor is nil, return the rightmost node in the tree
	if predecessor == nil {
		return bst.rightMostNode(bst.Root)
	}
	return predecessor
}

func (bst *BST) rightMostNode(node *TreeNode) *TreeNode {
	current := node
	for current.Right != nil {
		current = current.Right
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

type ReplicationJob struct {
	msg Message
	dst *TreeNode
}

type ReplicationQueue struct {
	Data []*ReplicationJob
	Lock sync.Mutex
}

func (q *ReplicationQueue) Add(val *ReplicationJob) {
	q.Data = append(q.Data, val)
}

func (q *ReplicationQueue) Empty() bool {
	return len(q.Data) == 0
}
