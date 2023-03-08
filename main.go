package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"github.com/multiformats/go-multicodec"
	"github.com/urfave/cli/v2"
)

const ()

var (
	signalChan     = make(chan os.Signal, 20)
	errInputNeeded = errors.New("no input file(s) given. See -h")
)

var description = `
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
	app.Description = description
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
				if !c.Args().Present() {
					return errInputNeeded
				}

				out := bufio.NewWriterSize(os.Stdout, 1024*1024)
				defer out.Flush()

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
							break
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
		{
			Name:      "stat",
			Usage:     "Provides some stats",
			ArgsUsage: "file1.car file2.car ...",
			Description: `
Lists total number of blocks, maximum and minimum block sizes and
other interesting facts about the CAR files.
`,
			Flags: []cli.Flag{},
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					return errInputNeeded
				}

				var nRoots, total, minSize, maxSize, avgSize, blocksSize uint64
				minSize = ^uint64(0)

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

					nRoots += uint64(len(h.Roots))

					for {
						_, b, err := util.ReadNode(buf)
						if err == io.EOF {
							break
						}
						if err != nil {
							return err
						}
						total++
						s := uint64(len(b))
						if s < minSize {
							minSize = s
						}
						if s > maxSize {
							maxSize = s
						}
						blocksSize += s
					}
					avgSize = blocksSize / total

				}

				fmt.Printf("blocks: %d\n", total)
				fmt.Printf("roots: %d\n", nRoots)
				fmt.Printf("size: %d B\n", blocksSize)
				fmt.Printf("min: %d B\n", minSize)
				fmt.Printf("max: %d B\n", maxSize)
				fmt.Printf("avg: %d B\n", avgSize)

				return nil
			},
		},
		{
			Name:      "overlay",
			Usage:     "Create an overlay CAR",
			ArgsUsage: "file1.car file2.car ...",
			Description: `
Generates an overlay CAR made of a DAG that references all the blocks in the
original CAR files as RAW-CID blocks.

Such CAR file allows to have a fully traversable DAG with a single root even
when the input CARs only provided incomplete DAGs: the overlay DAG reaches all
the blocks but, by declaring them as raw, ensure tooling will not attempt to
interpret them and therefore, attempt to follow links from them.

The overlay DAG can, for example, be use to recursively IPFS-pin all the
blocks in a CAR file even though when they correspond only to a partial
DAG where some links cannot be retrieved.

The overlay DAG IPLD nodes will grow until around 500kB at most.

The --shallow flag controls whether the resulting CAR includes the original
blocks too or only the overlay DAG blocks. In all cases, the resulting CAR
file will have a single root (the root of the overlay.
`,
			Flags: []cli.Flag{
				&cli.BoolFlag{
					Name:  "shallow",
					Usage: "Only include overlay-DAG blocks",
				},
				&cli.StringFlag{
					Name:    "output",
					Aliases: []string{"o"},
					Usage:   "Name of the CAR file to write to",
					Value:   "overlay.car",
				},
			},
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					return errInputNeeded
				}

				out, err := os.Create(c.String("output"))
				if err != nil {
					return err
				}
				defer out.Close()

				builder := cid.V1Builder{
					Codec:    uint64(multicodec.DagPb),
					MhType:   uint64(multicodec.Blake3),
					MhLength: 0,
				}

				// Write a dummy CAR header We will replace it
				// at the end with the actual header.
				// yes totally works.
				tempCid, err := builder.Sum([]byte("yeah"))
				if err != nil {
					return err
				}
				err = car.WriteHeader(&car.CarHeader{
					Roots:   []cid.Cid{tempCid},
					Version: 1,
				}, out)
				if err != nil {
					return err
				}

				bufout := bufio.NewWriterSize(out, 4<<20) // 4MiB

				root := merkledag.NodeWithData(nil)
				root.SetCidBuilder(builder)
				linkSizes := 0

			NEXT:
				for _, arg := range c.Args().Slice() {

					f, err := os.Open(arg)
					if err != nil {
						return err
					}
					defer f.Close()

					buf := bufio.NewReaderSize(f, 1<<20) // 1MiB
					_, err = car.ReadHeader(buf)
					if err != nil {
						return err
					}

					for {
						carBlockCid, data, err := util.ReadNode(buf)
						if err == io.EOF {
							break NEXT
						}
						if err != nil {
							return err
						}

						// Write node as we read it to the output
						if !c.Bool("shallow") {
							if err := util.LdWrite(bufout, carBlockCid.Bytes(), data); err != nil {
								return err
							}
						}

						rawCid := cid.NewCidV1(uint64(multicodec.Raw), carBlockCid.Hash())
						root.AddRawLink("", &format.Link{
							Size: uint64(len(data)),
							Cid:  rawCid,
						})
						linkSizes += rawCid.ByteLen()
						if linkSizes > 512*1024 { // 512KiB
							rootData, err := root.EncodeProtobuf(false)
							if err != nil {
								return err
							}
							rootCid := root.Cid()
							newRoot := merkledag.NodeWithData(nil)
							newRoot.SetCidBuilder(builder)
							newRoot.AddRawLink("more", &format.Link{
								Size: uint64(len(rootData)),
								Cid:  rootCid,
							})
							// flush full overlay-DAG-node to new CAR
							if err := util.LdWrite(bufout, root.Cid().Bytes(), root.RawData()); err != nil {
								return nil
							}

							root = newRoot
							linkSizes = 0
						}
					}
				}

				// flush final root node
				if err := util.LdWrite(bufout, root.Cid().Bytes(), root.RawData()); err != nil {
					return nil
				}

				err = bufout.Flush()
				if err != nil {
					return err
				}

				offset, err := out.Seek(0, 0)
				if err != nil {
					return err
				}
				if offset != 0 {
					return errors.New("cannot go back to the start of the file")
				}

				// This header should measure exactly the same
				// bytes as the bogus one.
				err = car.WriteHeader(&car.CarHeader{
					Roots:   []cid.Cid{root.Cid()},
					Version: 1,
				}, out)

				return err
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
