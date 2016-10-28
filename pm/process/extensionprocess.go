package process

import (
	"encoding/json"
	"github.com/g8os/core.base/pm/core"
	"github.com/g8os/core.base/pm/stream"
	"github.com/g8os/core.base/utils"
)

type extensionProcess struct {
	system Process
	cmd    *core.Command
}

func NewExtensionProcessFactory(exe string, dir string, args []string, env map[string]string) ProcessFactory {

	constructor := func(table PIDTable, cmd *core.Command) Process {
		sysargs := SystemCommandArguments{
			Name: exe,
			Dir:  dir,
			Args: args,
			Env:  env,
		}

		var input map[string]interface{}
		json.Unmarshal(cmd.Arguments, input)

		for _, arg := range args {
			sysargs.Args = append(sysargs.Args, utils.Format(arg, input))
		}

		payload, _ := json.Marshal(sysargs)

		extcmd := &core.Command{
			ID:        cmd.ID,
			Gid:       cmd.Gid,
			Nid:       cmd.Nid,
			Command:   CommandSystem,
			Arguments: json.RawMessage(payload),
			Tags:      cmd.Tags,
		}

		return &extensionProcess{
			system: NewSystemProcess(table, extcmd),
			cmd:    cmd,
		}
	}

	return constructor
}

func (process *extensionProcess) Cmd() *core.Command {
	return process.cmd
}

func (process *extensionProcess) Run() (<-chan *stream.Message, error) {
	return process.system.Run()
}

func (process *extensionProcess) Kill() {
	process.system.Kill()
}

func (process *extensionProcess) GetStats() *ProcessStats {
	return process.system.GetStats()
}
