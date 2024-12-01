package org.apache.pinot.tools;

import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotSegmentURIChanger extends PinotZKChanger{

  private final String _tableNameWithType;
  private final String _segmentName;
  private final SegmentZKMetadata _segmentZKMetadata;

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentURIChanger.class);

  private static void usage() {
    System.out.println("Usage: PinotSegmentURIChanger <zkAddress> <clusterName> <tableName> <segmentName> <newURI>");
    System.out.println("Example: localhost:2181 PinotCluster ....TBD....");
    System.exit(1);
  }

  public PinotSegmentURIChanger(final String zkAddress, final String clusterName,final String tableNameWithType, String segmentName) {
    super(zkAddress, clusterName);
    _tableNameWithType = tableNameWithType;
    _segmentName = segmentName;
    _segmentZKMetadata = ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, _tableNameWithType, _segmentName);
    assert _segmentZKMetadata != null;
  }

  public String getCurrentURL() {
    return _segmentZKMetadata.getDownloadUrl();
  }

  public void changeSegmentURI(final String newURI) {
    final String currentURL = _segmentZKMetadata.getDownloadUrl();

    if (currentURL.equals(newURI)) {
       LOGGER.info("Current and target URLs match - no need to update");
    } else {
      int version = _segmentZKMetadata.toZNRecord().getVersion();
      _segmentZKMetadata.setDownloadUrl(newURI);
      ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, _tableNameWithType, _segmentZKMetadata, version);
    }
  }

  public static void main(String[] args)
      throws Exception {
    final boolean dryRun = true;
    if (args.length != 5) {
      usage();
    }
    final String zkAddress = args[0];
    final String clusterName = args[1];
    final String tableName = args[2];
    final String segmentName = args[3];
    final String newURL = args[4];

    PinotSegmentURIChanger uriChanger = new PinotSegmentURIChanger(zkAddress, clusterName, tableName, segmentName);

    uriChanger.changeSegmentURI(newURL);
  }
}
