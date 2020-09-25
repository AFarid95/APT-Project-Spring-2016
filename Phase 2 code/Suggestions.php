<!DOCTYPE html>
<html>
<body>
<?php
header("Access-Control-Allow-Origin: *");
$q = $_GET['q'];
$server='VAIO-PC\SQLEXPRESS';
$connInfo=array("Database"=>"SearchEngineIndex","UID"=>"sa","PWD"=>"p@ssword13");
$con = sqlsrv_connect($server,$connInfo);
if (!$con) {
	echo "Connection could not be established.<br />";
    die( print_r( sqlsrv_errors(), true));
}
$sql="select top 5 Query from Queries where Query like '".$q."%' order by Frequency desc";
$result = sqlsrv_query($con,$sql);
while($row = sqlsrv_fetch_array($result)) {
	echo "<a href=\"#\" onclick=\"updateTextBox(this.innerHTML)\" style=\"font-size:14pt;\">".$row['Query']."</a><br>";
}
sqlsrv_close($con);
?>
</body>
</html>