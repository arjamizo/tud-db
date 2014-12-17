JFLAGS = -g -d .bin
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) -cp .bin:src $*.java

CLASSES = \
	src/Starter.java \

	# src/HashJoin.java \
	# src/Join.java \
	# src/NestedLoopJoin.java \
	# src/SortMergeJoin.java \
	# src/Triple.java \
	# src/Touple.java
	
run: classes
	mkdir -p .bin
	java -cp .bin Starter

default: run

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class
