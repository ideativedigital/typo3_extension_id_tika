<?php
if (!defined('TYPO3_MODE')) {
	die ('Access denied.');
}

// Scheduler to send customer requests to Campaign Commander
$GLOBALS['TYPO3_CONF_VARS']['SC_OPTIONS']['scheduler']['tasks'][\Ideative\IdTika\Task\IndexFileContent::class] = array(
    'extension'     => $_EXTKEY,
    'title'         => 'LLL:EXT:' . $_EXTKEY . '/Resources/Private/Language/locallang_db.xml:tx_cw_tika.title',
    'description' 	=> 'LLL:EXT:' . $_EXTKEY . '/Resources/Private/Language/locallang_db.xml:tx_cw_tika.description',
);
