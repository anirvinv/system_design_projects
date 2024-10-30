go build -race -buildmode=plugin ../mrapps/wc.go && go run -race mrcoordinator.go pg-*.txt | tee debug-log

go run -race mrworker.go wc.so &
go run -race mrworker.go wc.so &
go run -race mrworker.go wc.so &