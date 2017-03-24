package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/dghubble/go-twitter/twitter"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/clientcredentials"

	_ "gopkg.in/cq.v1"
)

type client struct {
	ID     string `toml:"id"`
	Secret string `toml:"secret"`
}

func main() {

	httpClient, err := getAuthClient()
	if err != nil {
		log.Fatal(err)
	}

	twitterClient := twitter.NewClient(httpClient)

	friendParams := &twitter.FriendListParams{
		ScreenName: "hoffa3",
		Count:      100,
	}

	search, _, err := twitterClient.Friends.List(friendParams)
	if err != nil {
		log.Fatal(err)
	}

	for _, user := range search.Users {
		friendParams := &twitter.FriendListParams{
			UserID: user.ID,
			Count:  100,
		}

	}

}

func getAuthClient() (*http.Client, error) {
	f, err := os.Open("config.toml")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var clientCred client
	if _, err := toml.DecodeReader(f, &clientCred); err != nil {
		return nil, err
	}
	fmt.Println(clientCred.Secret)

	config := clientcredentials.Config{
		ClientID:     clientCred.ID,
		ClientSecret: clientCred.Secret,
		TokenURL:     "https://api.twitter.com/oauth2/token",
	}

	ctx := context.Background()
	return config.Client(ctx), nil
}
