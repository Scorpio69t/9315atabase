# Makefile for COMP9315 23T1 Assignment 2

CC=gcc
CFLAGS=-std=gnu99 -Wall -g
OBJS=main.o ro.o db.o bufpool.o
BINS=main

main: $(OBJS)
	$(CC) -std=gnu99 -o main $(OBJS)

main.o: ro.h db.h

ro.o: ro.h db.h

db.o: db.h

bufpool.o: bufpool.h bufpool.c

clean:
	rm -f $(BINS) *.o
