package initter

import (
	"github.com/ava-labs/ortelius/utils/controlwrap"
)

type Starter interface {
	Start(sc controlwrap.ControlWrap) error
}
