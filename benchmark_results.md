## Benchmark

### Banchmark assumptions and prerequisities
Benchmark test was developed based on the following assumptions:
1. connection to kafka always should be set to keep alive to exclude unnecessary handshakes
2. internet is always slower thanlocal network (that is why I don't use for benchmark internet connection and instead read them HDD)
3. local network is always slower than localhost communication
4. localhost communication is always slower than memory access
5. file operations I would put on the same spot as local network
6. Inthis file I combined results for tests:
    1. without concurency
    2. with feeds reading concurency and ach feed has its own kafka producer
    3. with feeds reading concurency and fake kafka producers from 1 to 6 as performance does ot grows significantly
    4. with feeds reading concurence and kafka producer over the localhost to kafka (1 to 10 producers)
7. All benchmarks was done on laptop:
    1. with CPU Intel(R) Pentium(R) CPU 2020M @ 2.40GHz
    2. with 8 GB of ram
    3. with SSD as a storage
8. As feeds were used 2 files: first contains 107090 items in the feed and second contains 400000 items in the feed
As these files were quite large I did not include them into repo. The biggest one gzipped has 75MB. Instead I put 2 files with 10000 of items in each.

### Benchmarks results
1. No parallel processing of 2 feeds.

`commit 847eb6792fda83a536bf53da7a4fa92204444494`

Modifications which were done:
- disabled output of success processed message
```
BenchmarkRunOnce-2   	       1	166439885581 ns/op
PASS
ok  	github.com/grubastik/feeddo/cmd	166.450s
```

2. 2 feeds processing in parallel

`commit 3399e418824f398169c3eecfd8730699ce49535e`
```
BenchmarkRunOnce-2   	       1	128184099154 ns/op
PASS
ok  	github.com/grubastik/feeddo/cmd/feeddo	128.194s
```

3. 2 feeds with fake kafka producer. `commit current`

    1. single fake kafka producer
    ```
    BenchmarkRunOnce-2   	       1	134864168388 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	134.875s
    ```

    2. 2 fake kafka producer
    ```
    BenchmarkRunOnce-2   	       1	129825491076 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	129.836s
    ```

    3. 3 fake kafka producer
    ```
    BenchmarkRunOnce-2   	       1	129664247871 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	129.674s
    ```

    4. 4 fake kafka producer
    ```
    BenchmarkRunOnce-2   	       1	128522043421 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	128.532s
    ```

    5. 5 fake kafka producer
    ```
    BenchmarkRunOnce-2   	       1	129707580679 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	129.718s
    ```

    6. 6 fake kafka producer
    ```
    BenchmarkRunOnce-2   	       1	128823257527 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	128.833s
    ```

4. 2 feeds with 1 kafka producer and real kafka. `commit current`

    1. 1 kafka producer
    ```
    BenchmarkRunOnce-2   	       1	2130484814718 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	2130.514s
    ```

    2. 2 kafka producers
    ```
    BenchmarkRunOnce-2   	       1	1004193909837 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	1004.222s
    ```

    3. 3 kafka producers
    ```
    BenchmarkRunOnce-2   	       1	689246769010 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	689.271s
    ```

    4. 4 kafka producers
    ```
    BenchmarkRunOnce-2   	       1	538312303583 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	538.343s
    ```

    5. 5 kafka producers
    ```
    BenchmarkRunOnce-2   	       1	447547876633 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	447.579s
    ```

    6. 6 kafka producers
    ```
    BenchmarkRunOnce-2   	       1	386497672269 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	386.524s
    ```

    7. 7 kafka producers
    ```
    BenchmarkRunOnce-2   	       1	354696611997 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	354.737s
    ```

    8. 8 kafka producers
    ```
    BenchmarkRunOnce-2   	       1	339618006853 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	339.651s
    ```

    9. 9 kafka producers
    ```
    BenchmarkRunOnce-2   	       1	297272631624 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	297.302s
    ```

    10. 10 kafka producers
    ```
    BenchmarkRunOnce-2   	       1	291724956946 ns/op
    PASS
    ok  	github.com/grubastik/feeddo/cmd/feeddo	291.753s
    ```