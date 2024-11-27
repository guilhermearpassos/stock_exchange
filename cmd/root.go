package cmd

import (
	"github.com/spf13/cobra"
)

func Execute() error {

	c := &cobra.Command{
		Use: "se",
		RunE: func(cmd *cobra.Command, args []string) error {

			return cmd.Usage()
		},
	}

	c.AddCommand(ExecutorCmd)
	return c.Execute()
}
