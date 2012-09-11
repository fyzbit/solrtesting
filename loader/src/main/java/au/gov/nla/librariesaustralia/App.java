package au.gov.nla.librariesaustralia;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.marc4j.MarcReader;
import org.marc4j.MarcWriter;
import org.marc4j.MarcXmlReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Leader;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;

import org.xml.sax.SAXException;


/**
 * Test LA Loader
 */
public class App {
	
	private static SolrServer server;
	
	private static Date start = new Date();
	
	private static int total = 0;

	public static void main(String[] args) {
		server = new HttpSolrServer(args[0]);
		
		File dir = new File(args[1]);
		for (File d : dir.listFiles()) {
			if (d.isDirectory() && !d.isHidden()) {
				processDirectory(d);
			}
		}
	}

	private static void processDirectory(File d) {
		for (File f : d.listFiles()) {
			if (f.exists() && f.isFile()) {
				try {
					processFile(f);
					
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ParserConfigurationException e) {
					e.printStackTrace();
				} catch (SAXException e) {
					e.printStackTrace();
				} catch (TransformerConfigurationException e) {
					e.printStackTrace();
				} catch (TransformerException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

	private static void processFile(File f) throws IOException, FileNotFoundException, ParserConfigurationException, SAXException, TransformerException {
		String result = "";
		
		MarcReader reader = new MarcXmlReader(new GZIPInputStream(new FileInputStream(f)), App.class.getResource("fix.xslt").getFile());
		try {
			result = processRecords(reader);
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		
		System.out.println(f.getName() + " " + result);
	}

	private static String processRecords(MarcReader records) throws SolrServerException, IOException {
		
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (records.hasNext()) {
			processRecord(docs, records.next());
		}
		
		UpdateResponse response = server.add(docs);
		total += docs.size();
		
		long time = (new Date().getTime() - start.getTime()) / 1000;
		return "num:" + docs.size() + " status:" + response.getStatus() + " total:" + total + " time:" + time + " average:" + (total / time);
	}

	private static void processRecord(Collection<SolrInputDocument> docs, Record record) {
		SolrInputDocument doc = new SolrInputDocument();
		
		StringWriter out = new StringWriter(20000);
		MarcWriter writer = new MarcXmlWriter(new StreamResult(out));
		writer.write(record);
		
		doc.addField("record", out.toString().replaceAll("</?collection[^>]+>", ""));
		
    Leader leader = record.getLeader();
    char ldr6 = leader.getTypeOfRecord();
    char ldr7 = leader.getImplDefined1()[0];
    
    processLeader(doc, leader);

		for (Object d : record.getControlFields()) {
			ControlField controlField = (ControlField) d;
			String value = controlField.getData();
			
			int tagNo = Integer.parseInt(controlField.getTag(), 10);
			doc.addField("tag", tagNo);
			try {
				switch (tagNo) {
					case 1: {
						doc.setField("id", value);
						doc.setField("ANBDNo", value);
						break;
					}
					// case 5: doc.setField("lastmodified", value); break;
					
					case 6: {
						process006Tag(doc, value);
	        	break;
					}
					case 7 : {
						process007Tag(doc, value);
						break;
					}
					case 8: {
		        process008Tag(doc, ldr6, ldr7, value);
		        break;
					}
				}
			} catch (StringIndexOutOfBoundsException e) { 
				/* do nothing as the record is damaged */
				System.out.println(doc.getFieldValue("id") + " invalid " + tagNo + " tag: " + value.replace(' ', '#') + " " + e.toString());
			}
		}
		
		for (Object d : record.getDataFields()) {
			DataField dataField = (DataField) d;

			List<Subfield> subs = dataField.getSubfields();
			char ind1 = dataField.getIndicator1();
			char ind2 = dataField.getIndicator2();
			
			int tagNo = Integer.parseInt(dataField.getTag(), 10);
			if (tagNo == 880) {
				doc.addField("tag", tagNo);
				Subfield sub6 = dataField.getSubfield('6');
				if (sub6 != null) {
					tagNo = Integer.parseInt(sub6.getData().substring(0, 3), 10);
				}
			}
			
			doc.addField("tag", tagNo);
			switch(tagNo) {
				case 10: {
					doc.addField("LCControlNo", asStrings(subs, "abz"));
					break;
				}
				case 15:
				case 16:
				case 17:
				case 98: {
					doc.addField("OtherControlNo", asStrings(subs));
					break;
				}
				case 20: {
					doc.addField("ISBN", asStrings(subs, "az"));
					doc.addField("Note", asString(subs, "c"));
					break;
				}
				
				case 22: {
					doc.addField("ISSN", asStrings(subs, "ayz"));
					break;
				}
				
				case 24: {
					if (ind1 == '2') {
						doc.addField("ISMN", asString(subs, "ad"));
						doc.addField("ISMN", asString(subs, "z"));
					} else {
						doc.addField("OtherControlNo", asStrings(subs, "az"));
					}
					break;
				}
				
				case 27: {
					doc.addField("IdentifierISRN", asStrings(subs, "az"));
					break;
				}
			
				case 28: {
					doc.addField("MusicPublisherNo", asStrings(subs, "ab"));
					break;
				}		
			
				case 30: {
					doc.addField("CODEN No.", asStrings(subs, "az"));
					break;
				}
				
				case 40: {
					doc.addField("CatgAgencyOrig", asStrings(subs, "a"));
					doc.addField("CatgAgencyTrans", asStrings(subs, "c"));
					doc.addField("CatgAgencyModif", asStrings(subs, "d"));
					doc.addField("CatgDescriptConvent", asStrings(subs, "e"));
					break;
				}
				
				case 41: {
					doc.addField("Language", asStrings(subs, "abdefgh"));
					break;
				}
				
				case 42: {
					doc.addField("AustralianContentInd.", asStrings(subs, "a"));
					doc.addField("CatgAuthentCentre", asStrings(subs, "a"));
					break;
				}
				
				case 43: {
					doc.addField("GeographicAreaCode", asStrings(subs, "a"));
					break;
				}
				
				case 45: {
					doc.addField("TimePeriod", asStrings(subs));
					break;
				}
				
				case 46: {
					doc.addField("Date2", asStrings(subs, "de"));
					break;
				}
				
				case 47: {
					doc.addField("MSRFormofCompCde", asStrings(subs, "a"));
					break;
				}

				case 48: {
					doc.addField("MSRNumberofVoices", asStrings(subs, "ab"));
					break;
				}
				
				case 50: {
					doc.addField("LCClassification", asString(subs, "ab"));
					break;
				}
				
				case 60: {
					doc.addField("NLMClassification", asString(subs, "ab"));
					break;
				}
				
				case 66: {
					doc.addField("CharacterSet", asStrings(subs, "abc"));
					break;
				}
				
				case 70: {
					doc.addField("OtherClassification", asString(subs, "ab"));
					break;
				}
				
				case 72: {
					doc.addField("NALSubjectCategory", asString(subs, "ab", ""));
					break;
				}
				
				case 80: {
					doc.addField("UniversalDecimal", asString(subs, "abx", ""));
					break;
				}
				
				case 82: {
					doc.addField("DeweyClassification", asStrings(subs, "a"));
					doc.addField("ClassDeweyEdition", asStrings(subs, "2"));
					break;
				}
				
				case 86: {
					doc.addField("OtherControlNo", asStrings(subs, "a"));
					break;
				}
				
				case 88: {
					doc.addField("ReportNo", asStrings(subs, "az"));
					break;
				}
				
				case 91: {
					doc.addField("CollectionID", asStrings(subs, "a"));
					break;
				}
				
				case 99: {
					doc.addField("Immutable No.", asStrings(subs, "a"));
					break;
				}
				
				case 100:
				case 110: {
					doc.addField("NameRelatorCode", asStrings(subs, "4"));
					doc.addField("NameRelatorTerm", asStrings(subs, "e"));
					doc.addField("Name", asString(subs, "^4e"));
					doc.addField("Name_exact", asString(subs, "^4e"));
					doc.setField("Name_sort", asString(subs, "^4e"));
					break;
				}
				
				case 111: {
					doc.addField("NameRelatorCode", asStrings(subs, "4"));
					doc.addField("Name", asString(subs, "^4e"));
					doc.addField("Name_exact", asString(subs, "^4e"));
					doc.setField("Name_sort", asString(subs, "^4e"));
					break;
				}
				
				case 130:
				case 210:
				case 246:
				case 247: {
					doc.addField("Title", asString(subs));
					doc.addField("Title_exact", asString(subs));
					break;
				}
				
				case 240:
				case 242:
				case 243: {
					doc.addField("Title", asString(subs));
					doc.addField("Title_exact", asString(subs));
					break;
				}
				
				case 245: {
					doc.addField("Title", asString(subs));
					doc.addField("Title_exact", asString(subs));
					doc.setField("Title_sort", asString(subs).substring(Integer.parseInt("" + ind2)));
					break;
				}
				
				case 260: {
					doc.addField("CountryStateProv", asString(subs, "a"));
					doc.addField("PublName", asString(subs, "b"));
					doc.addField("PublName_exact", asString(subs, "b"));
					doc.addField("Note", asString(subs));
					break;
				}
				
				case 400:
				case 410:
				case 411: {
					doc.addField("Series", asString(subs));
					doc.addField("Series_exact", asString(subs));
					doc.addField("Name", asString(subs, "<t"));			  			// before t
					doc.addField("Name_exact", asString(subs, "<t"));			  // before t
					doc.addField("Title", asString(subs, ">t^xv"));					// after t except x,v
					doc.addField("Title_exact", asString(subs, ">t^xv"));		// after t except x,v
					break;
				}
				
				case 440: {
					doc.addField("ISSN", asStrings(subs, "x"));
					doc.addField("SeriesVolumeNo", asStrings(subs, "v"));
					doc.addField("Series", asString(subs, "^x"));
					doc.addField("Title", asString(subs, "^xv"));
					doc.addField("Title_exact", asString(subs, "^xv"));
					break;
				}
				
				case 490: {
					doc.addField("Series", asString(subs, "^x"));
					doc.addField("Series_exact", asString(subs, "^x"));
					doc.addField("Note", asString(subs, "^x"));
					doc.addField("Title", asString(subs, "^xv"));
					doc.addField("Title_exact", asString(subs, "^xv"));
					break;
				}
				
				case 505: {
					doc.addField("Note", asString(subs));
					doc.addField("Title", asString(subs, "at"));
					doc.addField("Title_exact", asString(subs, "at"));
					break;
				}
				
				case 600:
				case 610: {
					doc.addField("NameRelatorCode", asStrings(subs, "4"));
					doc.addField("NameRelatorTerm", asStrings(subs, "e"));
					doc.addField("SubjectSubdivision", asStrings(subs, "xyzv"));
					doc.addField("SubjectHeading", asString(subs, null, "--"));		// -- should only separate vxyz
					doc.addField("SubjectHeading_exact", asString(subs, null, "--"));		// -- should only separate vxyz
				}
				
				case 611: {
					doc.addField("NameRelatorCode", asStrings(subs, "4"));
					doc.addField("SubjectSubdivision", asStrings(subs, "xyzv"));
					doc.addField("SubjectHeading", asString(subs, null, "--"));		// -- should only separate vxyz
					doc.addField("SubjectHeading_exact", asString(subs, null, "--"));		// -- should only separate vxyz
				}
				
				case 630:
				case 648:
				case 650:
				case 651:
				case 653:
				case 654:
				case 655:
				case 656:
				case 657:
				case 658:
				case 752:
				case 754:
				case 755: {
					doc.addField("NameRelatorCode", asStrings(subs, "4"));
					doc.addField("SubjectSubdivision", asStrings(subs, "xyzv"));
					doc.addField("SubjectHeading", asString(subs, null, "--"));		// -- should only separate vxyz
					doc.addField("SubjectHeading_exact", asString(subs, null, "--"));		// -- should only separate vxyz
				}


				case 700:
				case 710: {
					doc.addField("NameRelatorCode", asStrings(subs, "4"));
					doc.addField("NameRelatorTerm", asStrings(subs, "e"));
					doc.addField("Name", asString(subs, "<t"));			  							// before t
					doc.addField("Name_exact", asString(subs, "<t"));			  							// before t
					doc.addField("Title", asString(subs, ">t^x"));									// after t except x
					doc.addField("Title_exact", asString(subs, ">t^x"));									// after t except x
					break;
				}

				case 711: {
					doc.addField("NameRelatorCode", asStrings(subs, "4"));
					doc.addField("Name", asString(subs, "<t"));			  							// before t
					doc.addField("Name_exact", asString(subs, "<t"));			  							// before t
					doc.addField("Title", asString(subs, ">t^x"));									// after t except x
					doc.addField("Title_exact", asString(subs, ">t^x"));									// after t except x
					break;
				}		
					
				case 720: {
					doc.addField("Name", asString(subs, "^x"));
					doc.addField("Name_exact", asString(subs, "^x"));
					break;
				}		
				
				case 730: {
					doc.addField("ISSN", asStrings(subs, "x"));
					doc.addField("Title", asString(subs, "^x"));
					doc.addField("Title_exact", asString(subs, "^x"));
					break;
				}	
				
				case 800:
				case 810: {
					doc.addField("NameRelatorCode", asStrings(subs, "4"));
					doc.addField("NameRelatorTerm", asStrings(subs, "e"));
					doc.addField("SeriesVolumeNo", asStrings(subs, "v"));
					doc.addField("Series", asString(subs));
					doc.addField("Series_exact", asString(subs));
					doc.addField("Name", asString(subs, "<t"));			  							// before t
					doc.addField("Name_exact", asString(subs, "<t"));			  							// before t
					doc.addField("Title", asString(subs, ">t^v"));									// after t except v
					doc.addField("Title_exact", asString(subs, ">t^v"));									// after t except v
				}
				
				case 811: {
					doc.addField("NameRelatorCode", asStrings(subs, "4"));
					doc.addField("SeriesVolumeNo", asStrings(subs, "v"));
					doc.addField("Series", asString(subs));
					doc.addField("Series_exact", asString(subs));
					doc.addField("Name", asString(subs, "<t"));			  							// before t
					doc.addField("Name_exact", asString(subs, "<t"));			  							// before t
					doc.addField("Title", asString(subs, ">t^v"));									// after t except v
					doc.addField("Title_exact", asString(subs, ">t^v"));									// after t except v
				}
				
				case 830:
				case 840: {
					doc.addField("SeriesVolumeNo", asStrings(subs, "v"));
					doc.addField("Series", asString(subs));
					doc.addField("Series_exact", asString(subs));
					doc.addField("Title", asString(subs, "^v"));									// after t except v
					doc.addField("Title_exact", asString(subs, "^v"));									// after t except v
				}
				
				case 850: {
					doc.addField("ILLLibrarySymbol", asStrings(subs, "a"));
					doc.addField("LocalSystemnumber", asStrings(subs, "b"));
					doc.addField("Holdingtext", asStrings(subs, "c"));
				}
				
				case 856: {
					doc.addField("Note", asStrings(subs, "c"));
					doc.addField("URUniformResourceId", asStrings(subs, "u"));
				}
				
		    case 13:
		    case 18:
		    case 33:
		    case 37:
		    case 250:
		    case 254:
		    case 255:
		    case 256:
		    case 257:
		    case 263:
		    case 270:
		    case 300:
		    case 306:
		    case 310:
		    case 321:
		    case 340:
		    case 351:
		    case 362:
		    case 363:
		    case 500:
		    case 501:
		    case 502:
		    case 503:
		    case 504:
		    case 506:
		    case 507:
		    case 508:
		    case 510:
		    case 511:
		    case 513:
		    case 514:
		    case 515:
		    case 516:
		    case 518:
		    case 520:
		    case 521:
		    case 522:
		    case 524:
		    case 525:
		    case 526:
		    case 530:
		    case 533:
		    case 534:
		    case 538:
		    case 540:
		    case 544:
		    case 545:
		    case 546:
		    case 547:
		    case 550:
		    case 552:
		    case 555:
		    case 556:
		    case 580:
		    case 581:
		    case 583:
		    case 590:
		    case 591:
		    case 592:
		    case 593:
		    case 594:
		    case 595:
		    case 596:
		    case 597:
		    case 598:
		    case 599:
		    case 753: {
		    	doc.addField("Note", asString(subs));
		    	break;
		    }

			}
			
			doc.addField("text", asString(subs));
		}
		
		docs.add(doc);
	}

	private static void process008Tag(SolrInputDocument doc, char ldr6, char ldr7, String value) {
		if (value.length() < 40) {
			System.out.println(doc.getFieldValue("id") + " invalid 008 tag: " + value.replace(' ', '#'));
		}
		
		String created = value.substring(0, 6);
		doc.addField("DateofEntry", created.charAt(0) < '6' ? "20" + created : "19" + created);
		doc.addField("PublicationStatus", value.charAt(6));
		doc.addField("Date2", value.substring(11, 15));
		doc.addField("CountryStateProv", value.substring(15, 18));
		doc.addField("GovtPublication", value.charAt(28));

		switch (ldr6) {
			case 'a':
		  case 't': {
		  	doc.addField("NatureofContent", value.substring(24, 28).toCharArray());
		  	doc.addField("ConferenceMeetingCode", value.charAt(29));
		  	doc.addField("FormofItem", value.charAt(23));
		  	switch (ldr7) {
		    	case 'a':
		    	case 'c':
		    	case 'd':
		    	case 'm': {
		    		doc.addField("BKBiography", value.charAt(34));
		    		doc.addField("BKFestschrift", value.charAt(30));
		    		doc.addField("BKIllustration Type", value.substring(18, 22).toCharArray());
		    		doc.addField("BKIndex Availabilty", value.charAt(31));
		    		doc.addField("BKLiterary Form", value.charAt(33));
		    		doc.addField("BKTarget Audience", value.charAt(22));
		        break;
		    	}
		    	case 'b':
		    	case 'i':
		    	case 's': {
		    		doc.addField("SEFormOriginalItem", value.charAt(22));
		    		doc.addField("SEFrequency", value.charAt(18));
		    		doc.addField("SEISDSCentre", value.charAt(20));
		    		doc.addField("SENatureEntireWork", value.charAt(24));
		    		doc.addField("SERegularity", value.charAt(19));
		    		doc.addField("SESuccLatestTitle", value.charAt(34));
		    		doc.addField("SETitleAlphabet", value.charAt(33));
		    		doc.addField("SEType", value.charAt(21));
		    		break;
		    	}
		  	}    
		    break;
		  }
		  case 'm': {
		  	doc.addField("CFTargetAudience", value.charAt(22));
		  	doc.addField("CFType", value.charAt(26));
		  	doc.addField("FormofItem", value.charAt(23));
		  	break;
		  }
		  case 'e':
		  case 'f': {
		  	doc.addField("CMFormat", value.substring(33, 35).toCharArray());
		  	doc.addField("CMIndexAvailablty", value.charAt(31));
		  	doc.addField("CMMapProjection", value.substring(22, 24));
		  	doc.addField("CMRelief", value.substring(18, 22));
		  	doc.addField("CMType", value.charAt(25));
		  	doc.addField("FormofItem", value.charAt(29));
		  	break;
		  }
		  case 'c':
		  case 'd':
		  case 'i':
		  case 'j': {
		  	doc.addField("MSRAccompg Text", value.substring(24, 30).toCharArray());
		  	doc.addField("MSRFormofMusComp", value.substring(18, 20));
		  	doc.addField("MSRFormat", value.charAt(20));
		  	doc.addField("MSRLiteraryText", value.substring(30, 32).toCharArray());
		  	doc.addField("MSRMusicparts", value.charAt(21));
		  	doc.addField("MSRTargetAudience", value.charAt(22));
		  	doc.addField("MSRTranspositionArr", value.charAt(33));
		  	doc.addField("FormofItem", value.charAt(23));
		  	break;
		  }
		  case 'g':
		  case 'k':
		  case 'o':
		  case 'r': {
		  	doc.addField("VMRunningTime", value.substring(18, 21));
		  	doc.addField("VMTargetAudience", value.charAt(22));
		  	doc.addField("VMTechnique", value.charAt(34));
		  	doc.addField("VMType", value.charAt(33));
		  	doc.addField("FormofItem", value.charAt(29));
		  	break;
		  }
		}
		
		doc.addField("Language", value.substring(35, 38));
		doc.addField("RecdModified", value.charAt(38));
		doc.addField("CatgSource", value.charAt(39));
	}

	private static void process006Tag(SolrInputDocument doc, String value) {
		doc.addField("GovtPublication", value.charAt(11));
		doc.addField("MaterialType", value.charAt(1));
		switch (value.charAt(0)) {
			case 'a':
			case 't': {
				doc.addField("BKBiography", value.charAt(17));
				doc.addField("BKFestschrift", value.charAt(13));
				doc.addField("BKIllustrationType", value.substring(1, 5).toCharArray());
				doc.addField("BKIndexAvailabilty", value.charAt(14));
				doc.addField("BKLiteraryForm", value.charAt(16));
				doc.addField("BKTargetAudience", value.charAt(5));
				doc.addField("ConferenceMeetingCode", value.charAt(12));
				doc.addField("NatureofContent", value.substring(7, 11).toCharArray());
				doc.addField("FormofItem", value.charAt(6));
				break;
			}
			case 'm': {
				doc.addField("CFTargetAudience", value.charAt(5));
				doc.addField("CFType", value.charAt(9));
		    doc.addField("FormofItem", value.charAt(6));
				break;
			}
			case 'e':
			case 'f': {
				doc.addField("CMFormat", value.substring(16, 18).toCharArray());
				doc.addField("CMIndexAvailablty", value.charAt(14));
				doc.addField("CMMapProjection", value.substring(5,  7));
				doc.addField("CMRelief", value.substring(1, 5).toCharArray());
				doc.addField("CMType", value.charAt(8));
		    doc.addField("FormofItem", value.charAt(12));
				break;
			}
			case 'c':
			case 'd':
			case 'i':
			case 'j': {
				doc.addField("MSRAccompgText", value.substring(6, 13).toCharArray());
				doc.addField("MSRFormofMusComp", value.substring(1, 3));
				doc.addField("MSRFormat", value.charAt(3));
				doc.addField("MSRLiteraryText", value.substring(13, 15).toCharArray());
				doc.addField("MSRMusicparts", value.charAt(4));
				doc.addField("MSRTargetAudience", value.charAt(5));
				doc.addField("MSRTranspositionArr", value.charAt(16));
				doc.addField("FormofItem", value.charAt(6));
				break;
			}
			case 's': {
				doc.addField("ConferenceMeetingCode", value.charAt(12));
				doc.addField("NatureofContent", value.substring(7, 11).toCharArray());
				doc.addField("SEFormOriginalItem", value.charAt(5));
				doc.addField("SEFrequency", value.charAt(1));
				doc.addField("SENatureEntireWork", value.charAt(7));
				doc.addField("SERegularity", value.charAt(2));
				doc.addField("SESuccLatestTitle", value.charAt(17));
				doc.addField("SETitleAlphabet", value.charAt(16));
				doc.addField("SEType", value.charAt(7));
				doc.addField("FormofItem", value.charAt(6));
				break;
			}
			case 'g':
			case 'k':
			case 'o':
			case 'r': {
				doc.addField("VMRunningTime", value.substring(1, 4));
				doc.addField("VMTargetAudience", value.charAt(5));
				doc.addField("VMTechnique", value.charAt(17));
				doc.addField("VMType", value.charAt(16));
				doc.addField("FormofItem", value.charAt(12));
				break;
			}
		}
	}

	private static void process007Tag(SolrInputDocument doc, String value) {
		switch (value.charAt(0)) {
			case 'c': {
				doc.addField("ERSMD", value.charAt(1));
				doc.addField("ERColour", value.charAt(3));
				doc.addField("ERDimension", value.charAt(4));
				doc.addField("ERSound", value.charAt(5));
				doc.addField("ERBitdepth", value.substring(6, 9));
				doc.addField("ERFormat", value.charAt(9));
				doc.addField("ERQualitytargets", value.charAt(10));
				doc.addField("ERAntecedent", value.charAt(1));
				doc.addField("ERCompression", value.charAt(12));
				doc.addField("ERReformatquality", value.charAt(13));
				break;
			}
			case 'd': {
				doc.addField("GLColour", value.charAt(3));
				doc.addField("GLPhysicalMedium", value.charAt(4));
				doc.addField("GLReprodType", value.charAt(5));
				doc.addField("GLSMD", value.charAt(1));
				break;
			}
			case 'h': {
				doc.addField("MICColour", value.charAt(9));
				doc.addField("MICDimension", value.charAt(4));
				doc.addField("MICEmulsion", value.charAt(10));
				doc.addField("MICFilmBase", value.charAt(12));
				doc.addField("MICGeneration", value.charAt(11));
				doc.addField("MICPolarity", value.charAt(3));
				doc.addField("MICReductionRange", value.charAt(5));
				doc.addField("MICReductionRatio", value.substring(6, 9));
				doc.addField("MICSMD", value.charAt(1));
		    break;
			}
			case 'm': {
				doc.addField("MPColour", value.charAt(3));
				doc.addField("MPDimension", value.charAt(7));
				doc.addField("MPMediumforSound", value.charAt(6));
				doc.addField("MPPresentnFormat", value.charAt(4));
				doc.addField("MPSMD", value.charAt(1));
				doc.addField("MPChanelConfiguration", value.charAt(8));
				doc.addField("MPProdElement", value.charAt(9));
				doc.addField("MPPosNegAspect", value.charAt(10));
				doc.addField("MPGeneration", value.charAt(11));
				doc.addField("MPSoundonMedSep", value.charAt(5));
				doc.addField("MPBaseofFilm", value.charAt(12));
				doc.addField("MPRefinedCatColour", value.charAt(13));
				doc.addField("MPKindColourStock", value.charAt(14));
				doc.addField("MPDeteriorationstage", value.charAt(15));
				doc.addField("MPCompleteness", value.charAt(16));
				doc.addField("MPInspectiondate", value.substring(17, 23));
		    break;
			}
			case 'a': {
				doc.addField("MapColour", value.charAt(3));
				doc.addField("MapPhysicalMedium", value.charAt(4));
				doc.addField("MapPolarity", value.charAt(7));
				doc.addField("MapProdnDetail", value.charAt(6));
				doc.addField("MapReprodType", value.charAt(5));
				doc.addField("MapSMD", value.charAt(1));
				break;
			}
			case 'k': {
				doc.addField("NPGColour", value.charAt(3));
				doc.addField("NPGPrimSportMatl", value.charAt(4));
				doc.addField("NPGSMD", value.charAt(1));
				doc.addField("NPGSecSupportMatl", value.charAt(5));
				break;
			}
			case 'g': {
				doc.addField("PGBaseEmulsnMatl", value.charAt(4));
				doc.addField("PGColour", value.charAt(3));
				doc.addField("PGDimension", value.charAt(7));
				doc.addField("PGMediumforSound", value.charAt(6));
				doc.addField("PGSMD", value.charAt(1));
				doc.addField("PGSecSupportMatl", value.charAt(8));
				doc.addField("PGSoundonMedSep", value.charAt(5));
				break;
			}
			case 'r': {
				doc.addField("RSSMD", value.charAt(1));
				doc.addField("RSAltitude", value.charAt(3));
				doc.addField("RSAttitude", value.charAt(4));
				doc.addField("RSCloudcover", value.charAt(5));
				doc.addField("RSPlatformtype", value.charAt(6));
				doc.addField("RSPlatformuse", value.charAt(7));
				doc.addField("RSSensortype", value.charAt(8));
				doc.addField("RSDatatype", value.substring(9, 11));
				break;
			}
			case 's': {
				doc.addField("SRCaptureStorage", value.charAt(13));
				doc.addField("SRChannelConfig", value.charAt(4));
				doc.addField("SR:Dimension", value.charAt(6));
				doc.addField("SRDiscGrooveWidth", value.charAt(5));
				doc.addField("SRDiscCylTape", value.charAt(9));
				doc.addField("SRKindofCutting", value.charAt(11));
				doc.addField("SRKindofMaterial", value.charAt(10));
				doc.addField("SRPlaybackChars", value.charAt(12));
				doc.addField("SRSMD", value.charAt(1));
				doc.addField("SRSpeed", value.charAt(3));
				doc.addField("SRTapeConfiguration", value.charAt(8));
				doc.addField("SRTapeWidth", value.charAt(7));
				break;
			}
			case 'f': {
				doc.addField("TMSMD", value.charAt(1));
				doc.addField("TMBraillefamily", value.substring(3, 5).toCharArray());
				doc.addField("TMContraction", value.charAt(5));
				doc.addField("TMMusicformat", value.substring(6, 9).toCharArray());
				doc.addField("TMPhysicalcharacteristics", value.charAt(9));
				break;
			}
			case 't': {
				doc.addField("TXSMD", value.charAt(1));
				break;
			}
			case 'v': {
				doc.addField("VRChannelConfig", value.charAt(8));
				doc.addField("VRColour", value.charAt(3));
				doc.addField("VRDimension", value.charAt(7));
				doc.addField("VRFormat", value.charAt(4));
				doc.addField("VRMediumforSound", value.charAt(6));
				doc.addField("VRSMD", value.charAt(1));
				doc.addField("VRSoundonMedSep", value.charAt(5));
				break;
			}
		}
	}

	private static void processLeader(SolrInputDocument doc, Leader leader) {
		doc.addField("RecdStatus", leader.getRecordStatus());
    doc.addField("MaterialType", leader.getTypeOfRecord());
    doc.addField("BibliographicLevel", leader.getImplDefined1()[0]);
    doc.addField("RecdTypeofControl", leader.getImplDefined1()[1]);
    doc.addField("CatgEncodingLevel", leader.getImplDefined2()[0]);
    doc.addField("CatgDescrForm", leader.getImplDefined2()[1]);
    doc.addField("RecdLinked", leader.getImplDefined2()[2]);
	}
	
	private static List<String> asStrings(List<Subfield> subs) {
		return asStrings(subs, null);
	}
	
	private static List<String> asStrings(List<Subfield> subfields, String codes) {
		List<String> out = new ArrayList<String>(2);
		
		Iterator<Subfield> i = subfields.iterator();
		while (i.hasNext()) {
			Subfield sub = i.next();
			
			if (codes == null || codes.contains(Character.toString(sub.getCode()))) {
				String data = sub.getData();
				if (data != null && !data.isEmpty()) {
					out.add(data);
				}
			}
		}
		
		return out;
	}

	private static String asString(List<Subfield> subfields) {
		return asString(subfields, null);
	}
	
	private static String asString(List<Subfield> subfields, String codes) {
		return asString(subfields, codes, " ");
	}
	
	private static String asString(List<Subfield> subfields, String codes, String sep) {
		String exclude = "56";
		String[] s = codes != null ? codes.split("\\^") : null;
		if (s != null) {
			codes = s[0].isEmpty() ? null : s[0];
			exclude = s.length == 2 ? s[1] : exclude; 
		}
		
		char swapProcess = '#';
		boolean process = true;
		if (codes != null && codes.startsWith("<")) {
			swapProcess = codes.charAt(1);
			codes = null;
		}
		if (codes != null && codes.startsWith(">")) {
			swapProcess = codes.charAt(1);
			process = false;
			codes = null;
		}
		
		
		StringBuffer out = new StringBuffer();
		
		String separator = "";
		Iterator<Subfield> i = subfields.iterator();
		while (i.hasNext()) {
			Subfield sub = i.next();
			
			if (sub.getCode() == swapProcess) {
				process = !process;
				swapProcess = '#';
			}
			
			String code = Character.toString(sub.getCode());
			String data = sub.getData();
			if (process && (codes == null || codes.contains(code)) && (exclude == null || !exclude.contains(code)) && !(data == null || data.isEmpty())) {
				out.append(separator);
				out.append(data);
				
				separator = sep;
			}
		}
		
		return out.length() > 0 ? out.toString() : null;
	}

}
