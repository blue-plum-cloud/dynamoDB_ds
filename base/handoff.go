package base

import (
	"config"
	"constants"
	"fmt"
	"sync/atomic"
	"time"
)

/* hijacks Start(), message commands processed is CLIENT_REQ_REVIVE only */
func (n *Node) busyWait(duration int, c *config.Config) {
	if c.DEBUG_LEVEL >= constants.INFO {
		fmt.Printf("\nbusyWait: %d killed for %d ms...\n", n.GetID(), duration)
	}
	reviveTime := time.Now().Add(time.Millisecond * time.Duration(duration))

	for {
		select {
		case <-n.close_ch:
			return

		case msg := <-n.rcv_ch: // consume rcv_ch and throw
			if msg.Command == constants.CLIENT_REQ_REVIVE {
				if c.DEBUG_LEVEL >= constants.INFO {
					fmt.Printf("\nbusyWait: %d reviving...\n", n.GetID())
				}
				return
			}

		case <-time.After(10 * time.Millisecond): // check in intervals of 10ms
			if time.Since(reviveTime) >= 0 {
				if c.DEBUG_LEVEL >= constants.INFO {
					fmt.Printf("\nbusyWait: %d reviving...\n", n.GetID())
				}
				return
			}

		}
	}
}

/* Send message to update the node with object, Keeps retrying forever.*/
func (n *Node) restoreHandoff(token *Token, msg Message, c *config.Config) {
	n.mutex.Lock()
	if _, exists := n.awaitAck[token.phy_id]; !exists {
		n.awaitAck[token.phy_id] = new(atomic.Bool)
	}
	n.awaitAck[token.phy_id].Store(true)
	n.channels[token.phy_id] <- msg
	n.mutex.Unlock()

	reqTime := time.Now()

	for {
		n.mutex.Lock()
		if !(n.awaitAck[token.phy_id].Load()) {
			if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
				fmt.Printf("restoreHandoff: %d->%d complete.\n", n.GetID(), token.phy_id)
			}
			delete(n.backup, token.phy_id)
			n.mutex.Unlock()
			return
		} else {
			n.mutex.Unlock()
		}
		if time.Since(reqTime) > time.Duration(config.SET_DATA_TIMEOUT_MS)*time.Millisecond {
			if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
				fmt.Printf("restoreHandoff: %d->%d timeout reached. Retrying...\n", n.GetID(), token.phy_id)
			}
			n.channels[token.phy_id] <- msg
			reqTime = time.Now()
		}
	}
}
