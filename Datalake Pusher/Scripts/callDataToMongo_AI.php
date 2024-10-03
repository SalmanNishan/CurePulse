<?php

if ($argc > 1) {
    $dateArgument = $argv[1];
    echo "Argument passed: $dateArgument";
} else {
    echo "No argument passed.";
}

require 'vendor/autoload.php';
date_default_timezone_set("Asia/Karachi");

//$startTime = "2023-11-06T00:00:00Z";
//$endTime = "2023-11-06T23:59:59Z";

$startTime = $dateArgument . "T00:00:00Z";
$endTime = $dateArgument . "T23:59:59Z";

//$startTime = date('Y-m-d\T00:00:00\Z', strtotime('-1 day'));
//$endTime = substr($startTime, 0, 10) . "T23:59:59Z";

// $startTime = date('Y-m-d\TH:i:00\Z', strtotime('-1 day'));
// $endTime = date('Y-m-d\TH:i:00\Z');

$organizationId = '014b8398-2222-9d52-2af8-000100420005';
// $userIDArray = ["95048b59-56fa-43ad-a063-629f272eecad", "c4975922-18b3-4552-b93d-0bd2de5d94e0", "fcad90e9-e2c7-4284-bcdb-30a6efb29b7d"];
$userIDArray = ["95048b59-56fa-43ad-a063-629f272eecad", "c4975922-18b3-4552-b93d-0bd2de5d94e0", "fcad90e9-e2c7-4284-bcdb-30a6efb29b7d", "da94b09c-3054-45fb-a36e-9c5ac1a8ba73", "ededd721-7845-4965-9a8c-96fdae8ab217", "a152b96f-e68d-4ffd-b75d-baa7192c2429", "2db90e22-5d3b-46a7-81b2-151e12879a49", "904e98a0-2bd0-47ce-85fb-0d4522b5079d", "8de0b006-4428-416a-aafd-84ac6778f5bd", "6135364e-42b0-4c46-8a5b-db207e796a95", "d2a82bf9-663c-468e-9d41-45a897b2810e", "4f027f10-e36a-4dda-9f63-e6617e570227"];
// $OAuthRefreshToken = "eyJraWQiOiI2MjAiLCJhbGciOiJSUzUxMiJ9.eyJzYyI6ImNhbGwtY29udHJvbC52MS5jYWxscy5jb250cm9sIGNhbGxzLnYyLmluaXRpYXRlIG1lc3NhZ2luZy52MS53cml0ZSBpZGVudGl0eTpzY2ltLm1lIGNhbGwtZXZlbnRzLnYxLmV2ZW50cy5yZWFkIG1lc3NhZ2luZy52MS5ub3RpZmljYXRpb25zLm1hbmFnZSB2b2ljZW1haWwudjEubm90aWZpY2F0aW9ucy5tYW5hZ2Ugc3VwcG9ydDogdm9pY2VtYWlsLnYxLnZvaWNlbWFpbHMud3JpdGUgZmF4LnYxLndyaXRlIHZvaWNlLWFkbWluLnYxLndyaXRlIGlkZW50aXR5OiB3ZWJydGMudjEucmVhZCB3ZWJydGMudjEud3JpdGUgY29sbGFiOiB2b2ljZS1hZG1pbi52MS5yZWFkIHByZXNlbmNlLnYxLnJlYWQgY2FsbC1ldmVudHMudjEubm90aWZpY2F0aW9ucy5tYW5hZ2UgaWRlbnRpdHk6c2NpbS5vcmcgcHJlc2VuY2UudjEud3JpdGUgZmF4LnYxLnJlYWQgY2FsbC1oaXN0b3J5LnYxLm5vdGlmaWNhdGlvbnMubWFuYWdlIHByZXNlbmNlLnYxLm5vdGlmaWNhdGlvbnMubWFuYWdlIG1lc3NhZ2luZy52MS5zZW5kIG1lc3NhZ2luZy52MS5yZWFkIGNyLnYxLnJlYWQgZmF4LnYxLm5vdGlmaWNhdGlvbnMubWFuYWdlIHVzZXJzLnYxLmxpbmVzLnJlYWQgdm9pY2VtYWlsLnYxLnZvaWNlbWFpbHMucmVhZCIsInN1YiI6Ijg0NTk4NjkzODQ1MTE5NTQ0NjciLCJhdWQiOiJjMzNmNjAzMC02MDg4LTQ5MzctYjgwZS00YmEzNzE0ZDY5ZjYiLCJvZ24iOiJwd2QiLCJ0eXAiOiJyIiwiZXhwIjoxNjk3NjA2OTYxLCJpYXQiOjE2OTUwMTQ5NjEsImp0aSI6IjVmYzBlMDdkLTQ1MTgtNDZjNi04YzcxLTUxN2UzNTMyZDUwYyJ9.HC_TcAuqP9Ba0rK-PPySQQ3UZDf89kyzNPgESy0RwsF0Z5eLulTnDzJpJPVPOug90ljx-dUVWnSARyJv_hKUOyPotWMHf-RYd0_Odawd5ajzpK4KLqImopFiosKgrScPCjYjFgbtpvsrs3ZHvHMu2stkJNd6n6QHSpgX_ielxCVWlphE_6cTT9dSBW4YVRXXhiIv9sRwuVcVelYsdkBX7n2910aDwwC2ng4hNtOVwiT7Nog4A_l66L4PI0ayfJ4kjyy_pFoVfWc7lBPXO5QNAbHyiZOf7NYDQPEiV-FsMfmYYk5vdkUN4_puclzFnq3cY8Dvy19ro4s8dzRROTGf-Q";
$OAuthCode = "eyJraWQiOiI2MjAiLCJhbGciOiJSUzUxMiJ9.eyJzYyI6ImNhbGwtY29udHJvbC52MS5jYWxscy5jb250cm9sIGNhbGxzLnYyLmluaXRpYXRlIG1lc3NhZ2luZy52MS53cml0ZSBpZGVudGl0eTpzY2ltLm1lIGNhbGwtZXZlbnRzLnYxLmV2ZW50cy5yZWFkIG1lc3NhZ2luZy52MS5ub3RpZmljYXRpb25zLm1hbmFnZSB2b2ljZW1haWwudjEubm90aWZpY2F0aW9ucy5tYW5hZ2Ugc3VwcG9ydDogdm9pY2VtYWlsLnYxLnZvaWNlbWFpbHMud3JpdGUgZmF4LnYxLndyaXRlIHZvaWNlLWFkbWluLnYxLndyaXRlIGlkZW50aXR5OiB3ZWJydGMudjEucmVhZCB3ZWJydGMudjEud3JpdGUgY29sbGFiOiB2b2ljZS1hZG1pbi52MS5yZWFkIHByZXNlbmNlLnYxLnJlYWQgY2FsbC1ldmVudHMudjEubm90aWZpY2F0aW9ucy5tYW5hZ2UgaWRlbnRpdHk6c2NpbS5vcmcgcHJlc2VuY2UudjEud3JpdGUgZmF4LnYxLnJlYWQgY2FsbC1oaXN0b3J5LnYxLm5vdGlmaWNhdGlvbnMubWFuYWdlIHByZXNlbmNlLnYxLm5vdGlmaWNhdGlvbnMubWFuYWdlIG1lc3NhZ2luZy52MS5zZW5kIG1lc3NhZ2luZy52MS5yZWFkIGNyLnYxLnJlYWQgZmF4LnYxLm5vdGlmaWNhdGlvbnMubWFuYWdlIHVzZXJzLnYxLmxpbmVzLnJlYWQgdm9pY2VtYWlsLnYxLnZvaWNlbWFpbHMucmVhZCIsInN1YiI6Ijg0NTk4NjkzODQ1MTE5NTQ0NjciLCJhdWQiOiJjMzNmNjAzMC02MDg4LTQ5MzctYjgwZS00YmEzNzE0ZDY5ZjYiLCJvZ24iOiJwd2QiLCJ0eXAiOiJyIiwiZXhwIjoxNzAwNjYxMTExLCJpYXQiOjE2OTgwNjkxMTEsImp0aSI6ImIyYWRiMjc3LWJmMzktNDRlNS04MGRiLWZhNzViNzczZGNjMCJ9.UlnyaGc3daM2Zff3a3jsEGbJ3By209NFAHlkJJkgtzkHpVG5Yx3YsDyr1MVmajAwq5EVSmwdKP_EFBIC5l0RP-e97vU-eshOfNTE4hoK_spC7gZ1xsNcgLMM6NtXQ6RjJbKM8yuND8VhZnNCtQNFgA8ClfA1kXGux7S_bBpNuKaSBUTE0Gf0LkOgi57aMkHJ29axkqHJjpSHtsXykleC8cQ409Ys-_IfhNjMXkQnEhn0LbzhqiGWA3k1gughr8HQ9sqFzafH37vfzSwKh6k1DvBOhU8C5gyIJ8L-Nsh-HLVJsRXvQWTCutu9vOXxyzcTYFQh40Tkhogoz2RrzIIV_A";
//$client = new MongoDB\Client('mongodb://localhost:27017');
$client = new MongoDB\Client('mongodb://curepulse_admin:Cure123pulse!*@172.16.101.152:27017/CurePulse?authMechanism=SCRAM-SHA-1');
$collectionData = $client->CurePulse->GotoAPI;
$collectionRefresh = $client->CurePulse->GotoAPI_RefreshToken;
$collectionAccess = $client->CurePulse->GotoAPI_AccessToken;
$timeStart = time();

// Getting the Refresh Token from the DB:
$localRefreshToken = $collectionRefresh->findOne([], ['sort' => ['_id' => -1]]);
$OAuthRefreshToken = $localRefreshToken['RefreshToken'];

function curlRequest($dataArray)
{
    $curl = curl_init();

    if (!$curl) {die("Failed to initialize!");}

    curl_setopt_array($curl, $dataArray);

    $response = curl_exec($curl);

    curl_close($curl);
    return json_decode($response);
}

$refreshTokenRequest = array(
    CURLOPT_URL => 'https://authentication.logmeininc.com/oauth/token',
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_ENCODING => '',
    CURLOPT_MAXREDIRS => 10,
    CURLOPT_TIMEOUT => 0,
    CURLOPT_FOLLOWLOCATION => true,
    CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
    CURLOPT_CUSTOMREQUEST => 'POST',
    // CURLOPT_POSTFIELDS => 'redirect_uri=http%3A%2F%2Fwww.example.com&grant_type=authorization_code&code=' . $OAuthCode,
    CURLOPT_POSTFIELDS => 'grant_type=refresh_token&refresh_token=' . $OAuthRefreshToken,
    CURLOPT_HTTPHEADER => array(
        'Content-Type: application/x-www-form-urlencoded',
        'Authorization: Basic YzMzZjYwMzAtNjA4OC00OTM3LWI4MGUtNGJhMzcxNGQ2OWY2OlVvREpSR2tQRDY5b1JDOExrUDhUTVRiNQ==',
    ),
);

// Saving Auth Token in the DB for logging purposes:
$OAuthToken = curlRequest($refreshTokenRequest);
$insertResult = $collectionAccess->insertOne((array) $OAuthToken);

if ($insertResult->getInsertedCount() === 1) {
    $collectionAccess->updateOne(['_id' => $insertResult->getInsertedId()], ['$set' => ['date' => date('Y-m-d H:i:s')]]);
}

foreach ($userIDArray as $userID) {
    start:
    $timeEnd = time();
    if (($timeEnd - $timeStart) > 3300) {
        echo "New Token Generated for use!\n";
        $OAuthToken = curlRequest($refreshTokenRequest);
        $insertResult = $collectionAccess->insertOne((array) $OAuthToken);
        if ($insertResult->getInsertedCount() === 1) {
            $collectionAccess->updateOne(['_id' => $insertResult->getInsertedId()], ['$set' => ['date' => date('Y-m-d H:i:s')]]);
        }
    }
    $userDataRequestArray = array(
        CURLOPT_URL => 'https://api.jive.com/call-reports/v1/reports/user-activity/' . $userID . '?startTime=' . $startTime . '&endTime=' . $endTime,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_ENCODING => '',
        CURLOPT_MAXREDIRS => 10,
        CURLOPT_TIMEOUT => 20,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_SSL_VERIFYHOST => false,

        CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
        CURLOPT_CUSTOMREQUEST => 'GET',
        CURLOPT_HTTPHEADER => array(
            'Authorization: Bearer ' . $OAuthToken->access_token,
        ),
    );

    $userDataArray = curlRequest($userDataRequestArray);
    if (!$userDataArray) {echo "Request Failed for this User! Retrying...\n";goto start;}
    echo "User Done: $userID\n";

    foreach ($userDataArray->items as $arrayKeys => $value) {
        $recordingID = null;
        $legId = $value->legId;
        if (isset($value->recordingIds[0])) {$recordingID = $value->recordingIds[0];}
        $callDirection = $value->direction;
        $calleeNumber = $value->callee->number;
        $callerNumber = $value->caller->number;
        if (!$calleeNumber) {$calleeNumber = 'NULL';}
        if (!$callerNumber) {$callerNumber = 'NULL';}
        $value = (array) $value;
        if ($recordingID) {
            $downloadRecordingArray = array(
                CURLOPT_URL => 'https://api.jive.com/call-reports/v1/recordings/' . $recordingID . '?organizationId=' . $organizationId,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_ENCODING => '',
                CURLOPT_MAXREDIRS => 10,
                CURLOPT_TIMEOUT => 20,
                CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_SSL_VERIFYPEER => false,
                CURLOPT_SSL_VERIFYHOST => false,
                CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
                CURLOPT_CUSTOMREQUEST => 'GET',
                CURLOPT_HTTPHEADER => array(
                    'Authorization: Bearer ' . $OAuthToken->access_token,
                ),
            );
            $recordingLinkArray = curlRequest($downloadRecordingArray);
            $value["RecordingURL"] = $recordingLinkArray->url;
            file_put_contents(dirname(__FILE__) . '/CallRecordings1/' . 'goto_' . $recordingID . '.wav', fopen($recordingLinkArray->url, 'r'));
        }
        $upsertDataResult = $collectionData->updateOne(array('legId' => $legId), array('$set' => $value), array('upsert' => true));
    }
}
