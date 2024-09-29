# Sliding Window Rate Limiter

## Logic overview

Each user has their own **redis sorted set** assigned to them in redis. It maintains sorted order of their requests based on the timestamp at which it arrived. To check if they exceeded the rate, remove all requests that are older than current_time - 1 second (or however long the interval is). The size of the set will tell you how many reuqests the user made in the past second.

## Concepts

1. Reverse proxy
   - Sits in front of the webserver and passes requests through to the target url
2. Redis
   - Central key value store
   - Allows the use of multiple instances of the rate limiter since state is stored here
3. Go - goroutines, channels
   - The HTTP server for the webserver and the rate limiter are run via goroutines
   - Channel is used so that http server can be gracefully shut down when the program recieves a SIGTERM
