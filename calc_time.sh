file="$1"
nr=$2
nt=$(($nr+1))

grep '^[0-9][0-9]:' "$file" \
| sort -g \
| awk -F: '
  BEGIN{
    s=e=0
  }
  (NR==1){
    s=$2*60+$3
  }
  (NR=='$nt'){
    e=$2*60+$3
  }
  END{
    print e-s" secs"
  }'

