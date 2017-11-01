#!/bin/bash

echo '
	set terminal png size 800, 600
	set output "read_workloada_fs.png"
	set title "read_throughput" font ",15"
	set key samplen 2  font ",15" top right box width 2 height 0.5 spacing 1.5
	set xlabel "time/2s" font ",15"
	set ylabel "MB/s" font ",15"
	plot "read_perf.txt" using 1 w l lt 3 lw 4 lc rgb "web-blue" title "hbase fs"
	exit
' | gnuplot

#web-blue
