#!/usr/bin/perl
use IO::File;
use POSIX qw(strftime);
$now = strftime "%a %b %e %H:%M:%S %Y", localtime;

#get the size_index hash map.
$file_size_index = new IO::File "../util_trace_size_def.h",'r';#O_RDWR;
my %size_index_map;
if (defined $file_size_index) {
        foreach my $line(<$file_size_index>)
	{
	    if($line=~/TRACE_SIZE_\d\[\]/)
	    {	
		@subset=split /_|\[|\]|=/, $line;
                $size_index_map{@subset[3]}=@subset[2];
		#my @keys = keys %size_index_map;
	    }
	}
        undef $file_size_index;       # automatically closes the file
}
#get the file and function name hash map
$file_src = new IO::File "../util_trace_content_def.h",'r';#O_RDWR;
my %src_name_map;
my %fun_name_map;
my %str_name_map;
my $file_start=0;
my $fun_start=0;
my $str_start=0;
my $index=0;
if (defined $file_src) {
        foreach my $line(<$file_src>)
	{
	    if($line=~/TRACE_FILE\[\]/)
	    {	
		#my @keys = keys %size_index_map;
		$file_start=1;
		$index=0;
	    }
	    if($line=~/TRACE_FUNCTION\[\]/)
	    {
		$file_start=0;
		$fun_start=1;
		$index=0;
	    }
	    if($line=~/TRACE_CONST_STR\[\]/)
	    {
		$file_start=0;
		$fun_start=0;
		$str_start=1;
		$index=0;
            }
	    if($file_start)
	    {
		@subset=split /"|"|=/, $line;
		
		if(@subset>2)
		{
		   $src_name_map{@subset[1]}=$index++;
		}
	    }
	    if($fun_start)
	    {
		@subset=split /"|"|=/, $line;
		if(@subset>2)
		{
		   $fun_name_map{@subset[1]}=$index++;
		}
	    }
	    if($str_start)
	    {
		@subset=split /"|"|=/, $line;
		if(@subset>2)
		{
		   $str_name_map{@subset[1]}=$index++;
		}
	    }
	}
        undef $file_src;       # automatically closes the file
}
my $root="../../../";
$index=0;
sub trim($)
{
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+{$//;
	return $string;
}
#the main replace toutine
sub do_the_replace($)
{
    my($file) = @_;
    $file_log_src = new IO::File $file,'r';#O_RDWR;
    if (defined $file_log_src) {
	$content=join("",<$file_log_src>);
	#$len=length($content);
	$last_fun="empty";
	print $file."\n";
	#while ($content =~ m/(.+\((\s*\)|(\s*\w+(\s|\*|\&)+\w+\s*[,\)])+)\s*{)|plat_log_msg/g) {
	while ($content =~ m/(.+\((\s*\)|(.*\w+\s+\w+.*\)))\s*{)|plat_log_msg/g) {
		if($&=~/plat_log_msg/)
		{
			print $last_fun."\n";
		}
		elsif($&!~/\//)
		{
			$last_fun=trim($&);
		}
	}
        foreach my $line(<$file_log_src>)
	{
		#if($line=~/(\w)+(\s)*\(((\s)+(\w)+(\s)+(\w)+(\s)*(,|\)))+[^;]/)
		#{
		    ##print $line;
		#}
		if($line=~/plat_log_msg/)
		{
			#remember the file name*/
			$name=substr $file,length($root);
			if(!defined $src_name_map{$name})
			{	
				#$src_name_map{$name}=$index;
				#$index++;
				#print $name."\n";
			}
			#remember the function name*/
			
		}
	}
        undef $file_log_src;       # automatically closes the file
    }
}
sub recurse_find($) {
  my($path) = @_;

  ## append a trailing / if it's not there
  $path .= '/' if($path !~ /\/$/);

  ## print the directory being searched
  ##print $path,"\n";

  ## loop through the files contained in the directory
  for my $eachFile (glob($path.'*')) {

    ## if the file is a directory
    if( -d $eachFile) {
      ## pass the directory to the routine ( recursion )
      recurse_find($eachFile);
    } else {

      ## print the file ... tabbed for readability
      if($eachFile=~/\.h|\.c/)
      {
	#print "\t",$eachFile,"\n";
	do_the_replace($eachFile);
      }
    }
  }
}
recurse_find($root);
print $index;
$data="test abc testdf abc tsdfabcdfe";
while ($data =~ m/abc/g) {
	print "Found '$&'";
}
##save all inform
#save the file and function name hash map
$src_header="#ifndef PLATFORM_UTIL_TRACE_CONTENT_DEF_H
#define PLATFORM_UTIL_TRACE_CONTENT_DEF_H 1

/*
 * File:   $HeadURL: svn://s002.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/util_content_size_def.h $
 * Author: Wei,Li
 *
 * Created On: ".$now."
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 * 
 * Notice: this system auto generated file, DO NOT MODIFY!!!
 */
";
$src_foot="
#endif /* ndef PLATFORM_TRACE_SIZE_DEF_H */";
$file_src = new IO::File "../util_trace_content_def.h",'w';#O_RDWR;
if (defined $file_src) {
        print $file_src $src_header;
	#print the file name
	print $file_src "static const char* TRACE_FILE[]={\n";
	foreach $key (sort {$src_name_map{$a}<=>$src_name_map{$b} }
           keys %src_name_map)
	{
     		print $file_src "\t\"".$key."\"\t\t/*THE FILE INDEX IS: ".$src_name_map{$key}."*/\n";
	}
	print $file_src "};\n";
	#print the funtion name
	print $file_src "static const char* TRACE_FUNCTION[]={\n";
	foreach $key (sort {$fun_name_map{$a}<=>$fun_name_map{$b} }
           keys %fun_name_map)
	{
     		print $file_src "\t\"".$key."\"\t\t/*THE FILE INDEX IS: ".$fun_name_map{$key}."*/\n";
	}
	print $file_src "};\n";
	#print the const string
	print $file_src "static const char* TRACE_CONST_STR[]={\n";
	foreach $key (sort {$str_name_map{$a}<=>$str_name_map{$b} }
           keys %str_name_map)
	{
     		print $file_src "\t\"".$key."\"\t\t/*THE FILE INDEX IS: ".$str_name_map{$key}."*/\n";
	}
	print $file_src "};\n";
	print $file_src $src_foot;
        undef $file_src;       # automatically closes the file
}