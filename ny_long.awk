BEGIN {FS=","; OFS=","} {if ($10 >= -77 && $10 <= -75 && $9 >= 42 && $9 <= 44) print $0} 
