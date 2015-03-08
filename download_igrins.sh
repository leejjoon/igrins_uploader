#!/bin/bash


if [ "$#" -ne 2 ]
then
 echo "usage download_igrins.sh [list file name] [outdir]"
 exit 1
fi

if [ ! -s "$1" ]
then
 echo "list file does not exist or empty"
 exit 1
fi

dir=$2


command -v curl >/dev/null 2>&1
if [ "$?" = "0" ]; then
  use_curl=1
else
  command -v wget >/dev/null 2>&1
  if [ "$?" = "0" ]; then
    use_curl=0
  else
    echo >&2 "Either curl or wget needs to be installed. Aborting."; exit 1;
  fi
fi


declare -a filename_list
declare -a md5sum_list
declare -a link_list

read -r headerline < $1

if [[ ! ${headerline:0:16} = "# IGRINS Listing" ]]
then
 echo "input file is not igrins listing file"
 exit 1
fi


if [[ ! -e $dir ]]; then
    mkdir $dir
elif [[ ! -d $dir ]]; then
    echo "$dir already exists but is not a directory" 1>&2
fi


count=0

while read -r -a input; do
 if [[ ! ${input:0:1} = "#" ]]; then
   if [ ${#input[@]} == 3 ]; then
     filename=$dir/${input[0]}
     if [ -e $filename ]; then
       echo "${input[2]} $filename" | md5sum -c --status -
       if [ "$?" = "0" ]; then
   	 echo "file already downloaded: $filename"
   	 continue
       fi
     fi

     filename_list[$count]=${input[0]}
     link_list[$count]=${input[1]}
     md5sum_list[$count]=${input[2]}

     count=$(( $count + 1 ))
   else
       echo "incorrect input line : " "${input[@]}"
   fi
 fi
done < $1

i=0
while [ $i -ne $count ]; do
  if [ $use_curl == 1 ]; then
    echo "downloading(w/ curl) $dir/${filename_list[$i]}"
    # -L to redirect to moved location
    curl -s -L -o $dir/${filename_list[$i]} "${link_list[$i]}"
  else
    echo "downloading(w/ wget) $dir/${filename_list[$i]}"
    wget -nv -O $dir/${filename_list[$i]} "${link_list[$i]}"
  fi

  i=$(( $i + 1 ))
done
