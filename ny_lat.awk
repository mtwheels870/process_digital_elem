BEGIN {FS=","; OFS=","} {if ($10 >= -77 && $10 <= -75) print $0} 
