<?php
/**
 * receiver.php — Save uploaded files to /public_html/data/ after verifying a shared secret.
 * Place this file on Hostinger at: /public_html/deploy/receiver.php
 * Ensure the /public_html/data directory exists and is writable.
 */
declare(strict_types=1);

$SHARED_SECRET = 'CHANGE_ME'; // set same as settings.json:webhook_secret
$DATA_DIR = __DIR__ . '/../data';

header('Content-Type: application/json');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['ok'=>false,'error'=>'Method not allowed']);
    exit;
}

$secret = $_POST['secret'] ?? '';
$filename = $_POST['filename'] ?? '';

if (!$secret || $secret !== $SHARED_SECRET) {
    http_response_code(403);
    echo json_encode(['ok'=>false,'error'=>'Forbidden']);
    exit;
}

if (!$filename || !isset($_FILES['file'])) {
    http_response_code(400);
    echo json_encode(['ok'=>false,'error'=>'Missing file or filename']);
    exit;
}

$allowed = [
    'sec_filings_snapshot.json',
    'sec_filings_snapshot.csv',
    'sec_filings_raw.json',
    'sec_debug_stats.json'
];
if (!in_array($filename, $allowed, true)) {
    http_response_code(400);
    echo json_encode(['ok'=>false,'error'=>'Filename not allowed']);
    exit;
}

if (!is_dir($DATA_DIR)) {
    @mkdir($DATA_DIR, 0755, true);
}

$dest = $DATA_DIR . '/' . basename($filename);
if (!move_uploaded_file($_FILES['file']['tmp_name'], $dest)) {
    http_response_code(500);
    echo json_encode(['ok'=>false,'error'=>'Failed to save file']);
    exit;
}

echo json_encode(['ok'=>true,'saved'=>$filename]);
