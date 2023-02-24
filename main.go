package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"github.com/urfave/cli/v2"
)

const ()

var (
	signalChan     = make(chan os.Signal, 20)
	errInputNeeded = errors.New("no input file(s) given. See -h")
)

var Description = `
caribdis is a command-line tool to work with CAR files.
`

func init() {
	go func() {
		signal.Notify(
			signalChan,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGHUP,
		)
		_, ok := <-signalChan // channel closed.
		if !ok {
			return
		}
		os.Exit(1)
	}()
}

func main() {
	app := cli.NewApp()
	app.Name = "caribdis"
	app.Usage = "CAR files passing by will be swallowed"
	app.UsageText = "caribdis [global options] [subcommand]..."
	app.Description = Description
	//app.Version = "latest"
	app.Flags = []cli.Flag{}

	app.Before = func(c *cli.Context) error {
		return nil
	}

	// app.Action = func(c *cli.Context) error {
	// 	return nil
	// }
	app.Commands = []*cli.Command{
		{
			Name:      "cat",
			Usage:     "Concatenate CAR files",
			ArgsUsage: "file1.car file2.car > merged.car",
			Description: `
Concatenates two or several CAR files by creating a CAR file that has the
blocks from all the given CAR files.

The resulting CAR file will have the roots set according to the --roots flag:
either it will include all the roots from all the CARs, the roots from the
first given CAR file, or those from the last.

Note blocks are not de-duplicated in the resulting archive, if they are present
in several of the input files.
`,
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "roots",
					Value: "all",
					Usage: "roots to include: [all, first, last]",
				},
			},
			Action: func(c *cli.Context) error {
				out := bufio.NewWriterSize(os.Stdout, 1024*1024)
				defer out.Flush()

				if !c.Args().Present() {
					return errInputNeeded
				}
				args := c.Args()
				lastF, err := os.Open(args.Get(args.Len() - 1))
				if err != nil {
					return (err)
				}
				defer lastF.Close()

				lastBuf := bufio.NewReader(lastF)
				lastHeader, err := car.ReadHeader(lastBuf)
				if err != nil {
					return err
				}

				finalHeader := &car.CarHeader{
					Roots:   lastHeader.Roots,
					Version: 1,
				}

				if err := car.WriteHeader(finalHeader, out); err != nil {
					return err
				}

				for _, carFileName := range args.Tail() {
					f, err := os.Open(carFileName)
					if err != nil {
						return err
					}

					buf := bufio.NewReaderSize(f, 1024*1024)
					_, err = car.ReadHeader(buf)
					if err != nil {
						return err
					}

					for {
						bs, err := util.LdRead(buf)
						if err == io.EOF {
							break
						}
						if err != nil {
							return err
						}

						err = util.LdWrite(out, bs)
						if err != nil {
							return err
						}
					}
				}
				return nil
			},
		},
		{
			Name:      "ls",
			Usage:     "List blocks or roots in CAR files",
			ArgsUsage: "file1.car file2.car ...",
			Description: `
Lists the blocks in the provided CAR files.
`,
			Flags: []cli.Flag{},
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					return errInputNeeded
				}

			LS_NEXT:
				for _, arg := range c.Args().Slice() {
					f, err := os.Open(arg)
					if err != nil {
						return err
					}
					defer f.Close()

					buf := bufio.NewReader(f)
					_, err = car.ReadHeader(buf)
					if err != nil {
						return err
					}

					for {
						c, _, err := util.ReadNode(buf)
						if err == io.EOF {
							break LS_NEXT
						}
						if err != nil {
							return err
						}
						fmt.Println(c)
					}
				}
				return nil
			},
		},
		{
			Name:      "roots",
			Usage:     "List roots in CAR files",
			ArgsUsage: "file1.car file2.car ...",
			Description: `
Lists the roots in the provided CAR files.
`,
			Flags: []cli.Flag{},
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					return errInputNeeded
				}

				for _, arg := range c.Args().Slice() {
					f, err := os.Open(arg)
					if err != nil {
						return err
					}
					defer f.Close()

					buf := bufio.NewReader(f)
					h, err := car.ReadHeader(buf)
					if err != nil {
						return err
					}

					for _, root := range h.Roots {
						fmt.Println(root)
					}
				}
				return nil
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
