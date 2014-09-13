DatasheetCompressor
===================
An open-source project for data type aware dataset compression, aiming to always outperform standard compression tools.

Command line usage
------------------
The syntax for the jar file is the following:

    java -jar DatasheetCompressor.jar <compress | decompress> -i <input file> [-o <output dir>] [-benchmark]
Which means that if you want to compress "example.csv" I will do the following:

    java -jar DatasheetCompressor.jar compress -i example.csv
And if you want to decompress "example.csv.cds" I will use the next syntax:

    java -jar DatasheetCompressor.jar decompress -i example.csv.cds

Adding the "benchmark" flag will print benchmarkings
Using the "o" flag will specify where to write the output file

Code usage
----------
To compress a file via code you should use the following code:

    new com.yonatankar.compressor.shell.CompressionManager().compress(<input file>, <output file>)
And to decompress a file via code you should use the following code:

    new com.yonatankar.compressor.shell.CompressionManager().decompress(<input file>, <output file>)
