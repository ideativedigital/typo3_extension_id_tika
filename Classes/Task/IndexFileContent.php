<?php

namespace Ideative\IdTika\Task;

use TYPO3\CMS\Backend\Utility\BackendUtility;
use TYPO3\CMS\Core\Database\ConnectionPool;
use TYPO3\CMS\Core\Database\Query\QueryBuilder;
use TYPO3\CMS\Core\Resource\ResourceFactory;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\CMS\Extbase\Configuration\ConfigurationManager;
use TYPO3\CMS\Extbase\Configuration\ConfigurationManagerInterface;
use TYPO3\CMS\Extbase\Object\ObjectManager;
use TYPO3\CMS\Scheduler\Task\AbstractTask;
use GuzzleHttp\Client;

class IndexFileContent extends AbstractTask
{
    const ALLOWED_EXTENSIONS = [
        'doc',
        'docx',
        'ppt',
        'pptx',
        'csv',
        'xls',
        'xlsx',
        'pdf',
        'rtf',
        'odt',
    ];

    const TIKA_URL_SUFFIX = '/update/extract?extractOnly=true&extractFormat=text';


    const TIKA_BATCH_SIZE = 50;

    /**
     * JMA don't list lebackend as a valid firstDomain
     * It's a copy of BackendUtility::firstDomainRecord($rootline) with an additionnal where clause
     * @params $rootLine
     */
    public static function firstDomainRecord($rootLine)
    {
        foreach ($rootLine as $row) {
            $dRec = BackendUtility::getRecordsByField('sys_domain', 'pid', $row['uid'],
                ' AND redirectTo=\'\' AND hidden=0 AND domainName NOT LIKE "%lebackend%"', '', 'sorting');
            if (is_array($dRec)) {
                $dRecord = reset($dRec);
                return rtrim($dRecord['domainName'], '/');
            }
        }
        return null;
    }

    /**
     * Get N (determined by the extension setting) records where the sys_file.`tstamp`
     * doesn't match the sys_file_metadata.`tx_idtika_processed_date`
     * (The file has been updated or it's a new one)
     *
     * Process it on our Tika microservice
     *
     * @return boolean Returns TRUE on successful execution, FALSE on error
     */
    public function execute()
    {
        $resourceFactory = ResourceFactory::getInstance();
        $extensionConfiguration = unserialize($GLOBALS['TYPO3_CONF_VARS']['EXT']['extConf']['id_tika']);

        /** @var ObjectManager $objectManager */
        $objectManager = GeneralUtility::makeInstance(ObjectManager::class);
        /** @var ConfigurationManager $configurationManager */
        $configurationManager = $objectManager->get(ConfigurationManager::class);
        $typoscriptConfig = $configurationManager->getConfiguration(ConfigurationManagerInterface::CONFIGURATION_TYPE_FULL_TYPOSCRIPT);

        $solrExtConfig = $typoscriptConfig['plugin.']['tx_solr.']['solr.'];
        $tikaURL = $solrExtConfig['scheme'] .
            '://' .
            trim($solrExtConfig['host'], '/') .
            (!empty($solrExtConfig['port']) ? ':' . $solrExtConfig['port'] : '') .
            '/'
            . trim($solrExtConfig['path'], '/') .
            '/' .
            trim(self::TIKA_URL_SUFFIX, '/');

        $batchSize = $extensionConfiguration['batchSize'];

        if (!isset($batchSize)) {
            $batchSize = self::TIKA_BATCH_SIZE;
        }

        /**
         * List of every file that must be indexed
         */
        /** @var QueryBuilder $qb */
        $qb = GeneralUtility::makeInstance(ConnectionPool::class)->getQueryBuilderForTable('sys_file');
        $rows = $qb->select('sys_file.uid', 'sys_file.sha1')
            ->from('sys_file')
            ->join('sys_file', 'sys_file_metadata', 'm',
                $qb->expr()->eq('m.file', $qb->quoteIdentifier('sys_file.uid')))
            ->join('sys_file', 'sys_file_storage', 's',
                $qb->expr()->eq('s.uid', $qb->quoteIdentifier('sys_file.storage')))
            ->where(
                $qb->expr()->eq('sys_file.missing', $qb->createNamedParameter(0, \PDO::PARAM_INT)),
                $qb->expr()->neq('sys_file.sha1', 'm.tx_idtika_processed_hash'),
                $qb->expr()->in('sys_file.extension', implode(',', array_map(function ($str) {
                    return sprintf("'%s'", $str);
                }, self::ALLOWED_EXTENSIONS)))
            )
            ->setMaxResults($batchSize)
            ->execute()
            ->fetchAll();


        if (is_array($rows)) {

            /** @var Client $client */
            $client = GeneralUtility::makeInstance(Client::class);

            foreach ($rows as $index => $row) {
                if (isset($rows[$index])) {
                    GeneralUtility::devLog(sprintf('Going to index %s records', count($rows) - $index), "id_tika");

                    $fileUID = $rows[$index]['uid'];
                    $file = $resourceFactory->getFileObject($fileUID);

                    GeneralUtility::devLog(sprintf("Going to process `%s`", $file->getIdentifier()), "id_tika");

                    try {
                        $response = $client->request('POST', $tikaURL, [
                            'multipart' => [
                                [
                                    'name' => 'FileContents',
                                    'contents' => $file->getContents(),
                                    'filename' => $file->getName()
                                ]
                            ],
                        ]);

                        if ($response->getStatusCode() === 200) {
                            if (strpos($response->getHeaderLine('Content-Type'), 'application/json') === 0) {
                                $content = $response->getBody()->getContents();
                                $jsonContent = json_decode($content, true);
                                if (isset($jsonContent[$file->getName()])) {
                                    $qb = GeneralUtility::makeInstance(ConnectionPool::class)->getQueryBuilderForTable('sys_file_metadata');

                                    $qb->update('sys_file_metadata')
                                        ->set('tx_idtika_processed_hash', $rows[$index]['sha1'])
                                        ->set('tx_idtika_text_content', trim($jsonContent[$file->getName()]))
                                        ->where(
                                            $qb->expr()->eq('file',
                                                $qb->createNamedParameter($fileUID, \PDO::PARAM_INT))
                                        )
                                        ->execute();

                                    // In order to avoid DDOSing the micro-service server
                                    sleep(2);
                                    GeneralUtility::devLog(sprintf("Done processing `%s`", $file->getIdentifier()),
                                        "id_tika");
                                } else {
                                    throw new \Exception("Missing information", 1523433352);
                                }
                            }
                        }
                    } catch (\Exception $ex) {
                        GeneralUtility::devLog(sprintf("Error : got message : `%s` for url `%s` (%s)",
                            $ex->getMessage(), $tikaURL, $file->getIdentifier()), "id_tika");
                    }
                }
            }
        }
        return true;
    }
}
