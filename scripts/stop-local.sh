while IFS='' read -r line || [[ -n "$line" ]] ; do
  kill $line
done < "pids"

rm pids

if [ "$1" = "y" ] || [ "$1" = "yes" ]; then
  echo "Deleting logs"
  rm -rf logs
fi
