package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/adjust/rmq/v5"
	"github.com/urfave/cli/v2"
)

func main() {

	connect, err := rmq.OpenConnection("my service", "tcp", "localhost:6379", 1, nil)
	if err != nil {
		log.Println(err)
	}
	taskQueue, err := connect.OpenQueue("tasks")
	if err != nil {
		log.Println(err)
	}

	app := &cli.App{
		Name:  "rmq",
		Usage: "runs a redis queue",
		Commands: []*cli.Command{
			{
				// go run main.go producer
				Name:  "producer",
				Usage: "creates redis queue producer",
				Action: func(ctx *cli.Context) error {

					i := 0
					go func() {
						ticker := time.NewTicker(1 * time.Second)
						for {
							select {
							case <-ticker.C:
								payload := fmt.Sprintf("tasks %d", i)
								fmt.Println(payload)
								if err := taskQueue.Publish(payload); err != nil {
									log.Println(err)
								}
								if i > 10 {
									fmt.Println("too many")
								}
							}
							i++
						}
					}()
					http.Handle("/overview", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						q, err := connect.GetOpenQueues()
						if err != nil {
							log.Println(err)
						}
						stats, err := connect.CollectStats(q)
						if err != nil {
							log.Println(err)
						}
						fmt.Fprintf(w, stats.GetHtml("", ""))
					}))

					http.Handle("/refresh", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						num, err := taskQueue.ReturnRejected(math.MaxInt)
						if err != nil {
							log.Println(err)
						}
						fmt.Fprintf(w, fmt.Sprintf("we return %d task from reject", num))
					}))

					if err := http.ListenAndServe(":3333", nil); err != nil {
						log.Fatal(err)
					}

					return nil
				},
			},
			{
				Name:  "consumer",
				Usage: "creates redis queue consumer",
				Action: func(ctx *cli.Context) error {
					taskQueue.StartConsuming(10, time.Second)
					taskQueue.AddConsumerFunc("task-consumer", func(d rmq.Delivery) {
						log.Printf("[%s]: performing task: %s", ctx.Args().First(), d.Payload())

						if err := d.Ack(); err != nil {
							log.Fatalln(err)
						}
						// if err := d.Reject(); err != nil {
						// 	log.Fatalln(err)
						// }
					})
					return nil
				},
			},
		},
	}

	app.Run(os.Args)
	time.Sleep(1 * time.Hour)
}
