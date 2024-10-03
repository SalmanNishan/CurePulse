<?php

date_default_timezone_set('America/New_York'); //Define Timezone
$serverIP = "172.16.101.152";
$body = "<h1>CurePulse Node 1 Server</h1>";
$body .= "<h3>Server IP: $serverIP</h3><hr>";
$body .= "<h2>CPU Stats</h2>";
$body .= "<table><tr><th style='outline : 1px solid black; width: 150px;'>Checked at</th><th style='outline : 1px solid black; width: 70px;'>CPU</th><th style='outline : 1px solid black; width: 70px;'>%usr</th><th style='outline : 1px solid black; width: 70px;'>%nice</th><th style='outline : 1px solid black; width: 70px;'>%sys</th><th style='outline : 1px solid black; width: 70px;'>%iowait</th><th style='outline : 1px solid black; width: 70px;'>%irq</th><th style='outline : 1px solid black; width: 70px;'>%soft</th><th style='outline : 1px solid black; width: 70px;'>%steal</th><th style='outline : 1px solid black; width: 70px;'>%guest</th><th style='outline : 1px solid black; width: 70px;'>%idle</th></tr><tr>";
$cpu_cmd = shell_exec("mpstat");
$cpu_usage = explode("\n", $cpu_cmd);
for ($i = 3; $i < (count($cpu_usage) - 1); $i++) {
    $line = $cpu_usage[$i];
    $usage = preg_replace('!\s+!', ' ', $line);
    $cells = explode(" ", $usage);
    unset($cells[10]);
    unset($cells[1]);
    foreach ($cells as $cell) {
        $body .= "<td style='outline: 1px solid black; text-align: center;'>$cell</td>";
    }
    $body .= "</tr>";
}
$body .= "</table>";

$body .= "<br>";
$body .= "<h2>RAM Stats</h2>";
$body .= "<table><tr><th style='outline : 1px solid black; width: 150px;'>Total</th><th style='outline : 1px solid black; width: 150px;'>Used</th><th style='outline : 1px solid black; width: 150px;'>Free</th><th style='outline : 1px solid black; width: 150px;'>Shared</th><th style='outline : 1px solid black; width: 150px;'>Buffers</th><th style='outline : 1px solid black; width: 150px;'>Cached</th></tr><tr>";
$ram_cmd = shell_exec("free -h");
$ram_usage = explode("\n", $ram_cmd);
$line = $ram_usage[1];
$usage = preg_replace('!\s+!', ' ', $line);
$cells = explode(" ", $usage);
for ($i = 1; $i < count($cells); $i++) {
    $cell = $cells[$i];
    $body .= "<td style='outline: 1px solid black; text-align: center;'>$cell</td>";
}
$body .= "</tr>";
$body .= "</table>";

$body .= "<br>";
$body .= "<h2>Disk Stats</h2>";
$body .= "<table><tr><th style='outline : 1px solid black; width: 150px;'>Filesystem</th><th style='outline : 1px solid black; width: 150px;'>Size</th><th style='outline : 1px solid black; width: 150px;'>Used</th><th style='outline : 1px solid black; width: 150px;'>Available</th><th style='outline : 1px solid black; width: 150px;'>Percentage Used</th><th style='outline : 1px solid black; width: 150px;'>Mounted on</th></tr><tr>";
$disk_cmd = shell_exec("df -h");
$disk_usage = explode("\n", $disk_cmd);
$diskArray = array('/dev/nvme0n1p3', '/dev/nvme1n1', '/dev/sda1');
for ($i = 1; $i < (count($disk_usage) - 1); $i++) {
    $line = $disk_usage[$i];
    if (is_numeric(strpos($line, $diskArray[0])) || is_numeric(strpos($line, $diskArray[1])) || is_numeric(strpos($line, $diskArray[2]))) {
        echo "$i: $line\n";
        $usage = preg_replace('!\s+!', ' ', $line);
        $cells = explode(" ", $usage);
        foreach ($cells as $cell) {
            $body .= "<td style='outline: 1px solid black; text-align: center;'>$cell</td>";
        }
        $body .= "</tr>";
    }
}
$body .= "</table>";

$body .= "<br>";
$body .= "Regards,<br>CurePulse Server Admin<br><br><b><u>Note: This is an automated email. Do not reply on this. </b></u>";

require_once '/home/cmdadmin/Datalake Pusher/CurePulse/server_stats_email/PHPMailer/class.phpmailer.php';
$mail = new PHPMailer();

// Settings
$mail->IsSMTP();
$mail->CharSet = 'UTF-8';

$mail->Host = "sendmail.curemd.com"; // SMTP server example
$mail->SMTPDebug = 1; // enables SMTP debug information (for testing)
$mail->SMTPAuth = false; // enable SMTP authentication
$mail->Port = 25; // set the SMTP port for the GMAIL server

$mail->SetFrom("serverstats@curemd.com", "CurePulse Server Stats");
// $mail->Username = ""; // SMTP account username example
// $mail->Password = ""; // SMTP account password example

$mail->AddAddress("saqlain.nawaz@curemd.com");
$mail->AddAddress("salman.nishan@curemd.com");
$mail->AddAddress("syed.obaid@curemd.com");
$mail->AddAddress("abdullah.sohail@curemd.com");
$mail->AddAddress("faizan.ahmad@curemd.com");

// Content
$mail->isHTML(true); // Set email format to HTML
$mail->Subject = "CurePulse Node 1 Server Stats";
$mail->Body = $body;
$mail->send();
