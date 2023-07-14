IMG_WIDTH=$1
IMG2_WIDTH=100
input=$(echo "$IMG_WIDTH $IMG2_WIDTH" | awk '{printf "%.4f \n", $1*$2}')
echo $input
