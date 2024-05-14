#!/usr/bin/env bash
PORT=8009
rand=0

function usage() {
  echo "usage $(basename $0) <data_file|cached> [rand|cons> [cnt_reqs] [query_params]"
  exit
}
[ -n "$4" ] && query_params="?${4}"

function getEmb() {
  i=$1
  l="${2}"
  out=`curl -sX POST -d "document=${l}" "http://127.0.0.1:${PORT}/${query_params}" \
  | tr -d '\000'`
  cnt_bytes=`echo "${out}" | wc -c`
  if [ $cnt_bytes -lt 2 ]; then
    echo "req #${i} failed"
  else
    echo -e "req #${i}: '${l}'\n\trecvd $cnt_bytes bytes: $(echo "${out}" | hexdump -n13 | awk '(NR==1) {$1="";print $0,"..."}')"
  fi
  echo `date +%T.%N`" END #${i}"
}
if [ "$1" = "cache" ]; then
  time getEmb 1 'The paper was made in Bohemia, I said.' &
  exit
fi

case "$2" in
  rand) rand=1 ;;
  cons) ;;
  *) usage ;;
esac
[ ! -f "$1" ] && usage

count_req=10
[ -n "$3" ] && count_req=$3
echo "count_req: ${count_req}"
sleep 1

if [ $rand -ne 0 ]; then
  e=`wc -l "$1" | cut -d' ' -f1`
fi

echo `date +%T.%N`' START LOOP'
for i in `seq 1 $count_req`; do
  if [ $rand -eq 0 ]; then
    ln=$i
  else
    ln=$((($RANDOM % $e)+1))
  fi
  l=`sed -n "${ln}p" "$1"`
  getEmb $i "${l}" &
done

