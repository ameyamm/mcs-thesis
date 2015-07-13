package com.ameyamm.mcs_thesis.input_generator

/**
 * @author ameya
 */
import scala.collection
import scala.collection.mutable
import scala.collection.immutable

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import com.ameyamm.mcs_thesis.ghsom.Instance
import com.ameyamm.mcs_thesis.ghsom.DimensionType
import com.ameyamm.mcs_thesis.ghsom.DoubleDimension
import com.ameyamm.mcs_thesis.model.Contact

class ElectionDatasetReader(private val datasetRDD : CassandraRDD[CassandraRow]) extends DatasetReader {

	//private val datasetRDD = sc.cassandraTable("uscensus1990", "dataset")

	private val _vectorSize = datasetRDD.first.size 
	private val datasetOfInstanceObjs = instanizeDataset(datasetRDD)

	def printDataset() {
		// println(datasetRDD.count)
		println(datasetOfInstanceObjs.first)
	}

	def vectorSize : Int = _vectorSize 

	def instanizeDataset(dataset : CassandraRDD[CassandraRow]) : RDD[Instance] = {

			val contactDataset = dataset.map( row => convertToContact(row) )

			val attribMap : RDD[(String,DoubleDimension)] = contactDataset.flatMap( 
							contact => { 
								List(
										("dage",contact.dage match { case Some(value) => value ; case None => null }),
										("dancstry1",contact.dancstry1 match { case Some(value) => value ; case None => null }),
										("dancstry2",contact.dancstry2 match { case Some(value) => value ; case None => null }),
										("ddepart",contact.ddepart match { case Some(value) => value ; case None => null }),
										("dhispanic",contact.dhispanic match { case Some(value) => value ; case None => null }),
										("dhour89",contact.dhour89 match { case Some(value) => value ; case None => null }),
										("dhours",contact.dhours match { case Some(value) => value ; case None => null }),
										("dincome1",contact.dincome1 match { case Some(value) => value ; case None => null }),
										("dincome2",contact.dincome2 match { case Some(value) => value ; case None => null }),
										("dincome3",contact.dincome3 match { case Some(value) => value ; case None => null }),
										("dincome4",contact.dincome4 match { case Some(value) => value ; case None => null }),
										("dincome5",contact.dincome5 match { case Some(value) => value ; case None => null }),
										("dincome6",contact.dincome6 match { case Some(value) => value ; case None => null }),
										("dincome7",contact.dincome7 match { case Some(value) => value ; case None => null }),
										("dincome8",contact.dincome8 match { case Some(value) => value ; case None => null }),
										("dindustry",contact.dindustry match { case Some(value) => value ; case None => null }),
										("doccup",contact.doccup match { case Some(value) => value ; case None => null }),
										("dpob",contact.dpob match { case Some(value) => value ; case None => null }),
										("dpoverty",contact.dpoverty match { case Some(value) => value ; case None => null }),
										("dpwgt1",contact.dpwgt1 match { case Some(value) => value ; case None => null }),
										("drearning",contact.drearning match { case Some(value) => value ; case None => null }),
										("drpincome",contact.drpincome match { case Some(value) => value ; case None => null }),
										("dtravtime",contact.dtravtime match { case Some(value) => value ; case None => null }),
										("dweek89",contact.dweek89 match { case Some(value) => value ; case None => null }),
										("dyrsserv",contact.dyrsserv match { case Some(value) => value ; case None => null }),
										("iavail",contact.iavail match { case Some(value) => value ; case None => null }),
										("icitizen",contact.icitizen match { case Some(value) => value ; case None => null }),
										("iclass",contact.iclass match { case Some(value) => value ; case None => null }),
										("idisabl1",contact.idisabl1 match { case Some(value) => value ; case None => null }),
										("idisabl2",contact.idisabl2 match { case Some(value) => value ; case None => null }),
										("ienglish",contact.ienglish match { case Some(value) => value ; case None => null }),
										("ifeb55",contact.ifeb55 match { case Some(value) => value ; case None => null }),
										("ifertil",contact.ifertil match { case Some(value) => value ; case None => null }),
										("iimmigr",contact.iimmigr match { case Some(value) => value ; case None => null }),
										("ikorean",contact.ikorean match { case Some(value) => value ; case None => null }),
										("ilang1",contact.ilang1 match { case Some(value) => value ; case None => null }),
										("ilooking",contact.ilooking match { case Some(value) => value ; case None => null }),
										("imarital",contact.imarital match { case Some(value) => value ; case None => null }),
										("imay75880",contact.imay75880 match { case Some(value) => value ; case None => null }),
										("imeans",contact.imeans match { case Some(value) => value ; case None => null }),
										("imilitary",contact.imilitary match { case Some(value) => value ; case None => null }),
										("imobility",contact.imobility match { case Some(value) => value ; case None => null }),
										("imobillim",contact.imobillim match { case Some(value) => value ; case None => null }),
										("iothrserv",contact.iothrserv match { case Some(value) => value ; case None => null }),
										("iperscare",contact.iperscare match { case Some(value) => value ; case None => null }),
										("iragechld",contact.iragechld match { case Some(value) => value ; case None => null }),
										("irelat1",contact.irelat1 match { case Some(value) => value ; case None => null }),
										("irelat2",contact.irelat2 match { case Some(value) => value ; case None => null }),
										("iremplpar",contact.iremplpar match { case Some(value) => value ; case None => null }),
										("iriders",contact.iriders match { case Some(value) => value ; case None => null }),
										("irlabor",contact.irlabor match { case Some(value) => value ; case None => null }),
										("irownchld",contact.irownchld match { case Some(value) => value ; case None => null }),
										("irpob",contact.irpob match { case Some(value) => value ; case None => null }),
										("irrelchld",contact.irrelchld match { case Some(value) => value ; case None => null }),
										("irspouse",contact.irspouse match { case Some(value) => value ; case None => null }),
										("irvetserv",contact.irvetserv match { case Some(value) => value ; case None => null }),
										("ischool",contact.ischool match { case Some(value) => value ; case None => null }),
										("isept80",contact.isept80 match { case Some(value) => value ; case None => null }),
										("isex",contact.isex match { case Some(value) => value ; case None => null }),
										("isubfam1",contact.isubfam1 match { case Some(value) => value ; case None => null }),
										("isubfam2",contact.isubfam2 match { case Some(value) => value ; case None => null }),
										("itmpabsnt",contact.itmpabsnt match { case Some(value) => value ; case None => null }),
										("ivietnam",contact.ivietnam match { case Some(value) => value ; case None => null }),
										("iwork89",contact.iwork89 match { case Some(value) => value ; case None => null }),
										("iworklwk",contact.iworklwk match { case Some(value) => value ; case None => null }),
										("iwwii",contact.iwwii match { case Some(value) => value ; case None => null }),
										("iyearsch",contact.iyearsch match { case Some(value) => value ; case None => null }),
										("iyearwrk",contact.iyearwrk match { case Some(value) => value ; case None => null })
										)
							} 
			)

			val maxVector = attribMap.reduceByKey( DoubleDimension.getMax _).collectAsMap()
			val minVector = attribMap.reduceByKey( DoubleDimension.getMin _).collectAsMap()

			contactDataset.map ( 
									contact => new Contact(
											caseid = contact.caseid,
											dage = 
											contact.dage match { 
											case Some(value) => Some((value - maxVector("dage"))/(maxVector("dage") - minVector("dage"))); 
											case None => None 
											},
											dancstry1 = 
											contact.dancstry1 match { 
											case Some(value) => Some((value - maxVector("dancstry1"))/(maxVector("dancstry1") - minVector("dancstry1"))); 
											case None => None 
											},
											dancstry2 = 
											contact.dancstry2 match { 
											case Some(value) => Some((value - maxVector("dancstry2"))/(maxVector("dancstry2") - minVector("dancstry2"))); 
											case None => None 
											},
											ddepart = 
											contact.ddepart match { 
											case Some(value) => Some((value - maxVector("ddepart"))/(maxVector("ddepart") - minVector("ddepart"))); 
											case None => None 
											},
											dhispanic = 
											contact.dhispanic match { 
											case Some(value) => Some((value - maxVector("dhispanic"))/(maxVector("dhispanic") - minVector("dhispanic"))); 
											case None => None 
											},
											dhour89 = 
											contact.dhour89 match { 
											case Some(value) => Some((value - maxVector("dhour89"))/(maxVector("dhour89") - minVector("dhour89"))); 
											case None => None 
											},
											dhours = 
											contact.dhours match { 
											case Some(value) => Some((value - maxVector("dhours"))/(maxVector("dhours") - minVector("dhours"))); 
											case None => None 
											},
											dincome1 = 
											contact.dincome1 match { 
											case Some(value) => Some((value - maxVector("dincome1"))/(maxVector("dincome1") - minVector("dincome1"))); 
											case None => None 
											},
											dincome2 = 
											contact.dincome2 match { 
											case Some(value) => Some((value - maxVector("dincome2"))/(maxVector("dincome2") - minVector("dincome2"))); 
											case None => None 
											},
											dincome3 = 
											contact.dincome3 match { 
											case Some(value) => Some((value - maxVector("dincome3"))/(maxVector("dincome3") - minVector("dincome3"))); 
											case None => None 
											},
											dincome4 = 
											contact.dincome4 match { 
											case Some(value) => Some((value - maxVector("dincome4"))/(maxVector("dincome4") - minVector("dincome4"))); 
											case None => None 
											},
											dincome5 = 
											contact.dincome5 match { 
											case Some(value) => Some((value - maxVector("dincome5"))/(maxVector("dincome5") - minVector("dincome5"))); 
											case None => None 
											},
											dincome6 = 
											contact.dincome6 match { 
											case Some(value) => Some((value - maxVector("dincome6"))/(maxVector("dincome6") - minVector("dincome6"))); 
											case None => None 
											},
											dincome7 = 
											contact.dincome7 match { 
											case Some(value) => Some((value - maxVector("dincome7"))/(maxVector("dincome7") - minVector("dincome7"))); 
											case None => None 
											},
											dincome8 = 
											contact.dincome8 match { 
											case Some(value) => Some((value - maxVector("dincome8"))/(maxVector("dincome8") - minVector("dincome8"))); 
											case None => None 
											},
											dindustry = 
											contact.dindustry match { 
											case Some(value) => Some((value - maxVector("dindustry"))/(maxVector("dindustry") - minVector("dindustry"))); 
											case None => None 
											},
											doccup = 
											contact.doccup match { 
											case Some(value) => Some((value - maxVector("doccup"))/(maxVector("doccup") - minVector("doccup"))); 
											case None => None 
											},
											dpob = 
											contact.dpob match { 
											case Some(value) => Some((value - maxVector("dpob"))/(maxVector("dpob") - minVector("dpob"))); 
											case None => None 
											},
											dpoverty = 
											contact.dpoverty match { 
											case Some(value) => Some((value - maxVector("dpoverty"))/(maxVector("dpoverty") - minVector("dpoverty"))); 
											case None => None 
											},
											dpwgt1 = 
											contact.dpwgt1 match { 
											case Some(value) => Some((value - maxVector("dpwgt1"))/(maxVector("dpwgt1") - minVector("dpwgt1"))); 
											case None => None 
											},
											drearning = 
											contact.drearning match { 
											case Some(value) => Some((value - maxVector("drearning"))/(maxVector("drearning") - minVector("drearning"))); 
											case None => None 
											},
											drpincome = 
											contact.drpincome match { 
											case Some(value) => Some((value - maxVector("drpincome"))/(maxVector("drpincome") - minVector("drpincome"))); 
											case None => None 
											},
											dtravtime = 
											contact.dtravtime match { 
											case Some(value) => Some((value - maxVector("dtravtime"))/(maxVector("dtravtime") - minVector("dtravtime"))); 
											case None => None 
											},
											dweek89 = 
											contact.dweek89 match { 
											case Some(value) => Some((value - maxVector("dweek89"))/(maxVector("dweek89") - minVector("dweek89"))); 
											case None => None 
											},
											dyrsserv = 
											contact.dyrsserv match { 
											case Some(value) => Some((value - maxVector("dyrsserv"))/(maxVector("dyrsserv") - minVector("dyrsserv"))); 
											case None => None 
											},
											iavail = 
											contact.iavail match { 
											case Some(value) => Some((value - maxVector("iavail"))/(maxVector("iavail") - minVector("iavail"))); 
											case None => None 
											},
											icitizen = 
											contact.icitizen match { 
											case Some(value) => Some((value - maxVector("icitizen"))/(maxVector("icitizen") - minVector("icitizen"))); 
											case None => None 
											},
											iclass = 
											contact.iclass match { 
											case Some(value) => Some((value - maxVector("iclass"))/(maxVector("iclass") - minVector("iclass"))); 
											case None => None 
											},
											idisabl1 = 
											contact.idisabl1 match { 
											case Some(value) => Some((value - maxVector("idisabl1"))/(maxVector("idisabl1") - minVector("idisabl1"))); 
											case None => None 
											},
											idisabl2 = 
											contact.idisabl2 match { 
											case Some(value) => Some((value - maxVector("idisabl2"))/(maxVector("idisabl2") - minVector("idisabl2"))); 
											case None => None 
											},
											ienglish = 
											contact.ienglish match { 
											case Some(value) => Some((value - maxVector("ienglish"))/(maxVector("ienglish") - minVector("ienglish"))); 
											case None => None 
											},
											ifeb55 = 
											contact.ifeb55 match { 
											case Some(value) => Some((value - maxVector("ifeb55"))/(maxVector("ifeb55") - minVector("ifeb55"))); 
											case None => None 
											},
											ifertil = 
											contact.ifertil match { 
											case Some(value) => Some((value - maxVector("ifertil"))/(maxVector("ifertil") - minVector("ifertil"))); 
											case None => None 
											},
											iimmigr = 
											contact.iimmigr match { 
											case Some(value) => Some((value - maxVector("iimmigr"))/(maxVector("iimmigr") - minVector("iimmigr"))); 
											case None => None 
											},
											ikorean = 
											contact.ikorean match { 
											case Some(value) => Some((value - maxVector("ikorean"))/(maxVector("ikorean") - minVector("ikorean"))); 
											case None => None 
											},
											ilang1 = 
											contact.ilang1 match { 
											case Some(value) => Some((value - maxVector("ilang1"))/(maxVector("ilang1") - minVector("ilang1"))); 
											case None => None 
											},
											ilooking = 
											contact.ilooking match { 
											case Some(value) => Some((value - maxVector("ilooking"))/(maxVector("ilooking") - minVector("ilooking"))); 
											case None => None 
											},
											imarital = 
											contact.imarital match { 
											case Some(value) => Some((value - maxVector("imarital"))/(maxVector("imarital") - minVector("imarital"))); 
											case None => None 
											},
											imay75880 = 
											contact.imay75880 match { 
											case Some(value) => Some((value - maxVector("imay75880"))/(maxVector("imay75880") - minVector("imay75880"))); 
											case None => None 
											},
											imeans = 
											contact.imeans match { 
											case Some(value) => Some((value - maxVector("imeans"))/(maxVector("imeans") - minVector("imeans"))); 
											case None => None 
											},
											imilitary = 
											contact.imilitary match { 
											case Some(value) => Some((value - maxVector("imilitary"))/(maxVector("imilitary") - minVector("imilitary"))); 
											case None => None 
											},
											imobility = 
											contact.imobility match { 
											case Some(value) => Some((value - maxVector("imobility"))/(maxVector("imobility") - minVector("imobility"))); 
											case None => None 
											},
											imobillim = 
											contact.imobillim match { 
											case Some(value) => Some((value - maxVector("imobillim"))/(maxVector("imobillim") - minVector("imobillim"))); 
											case None => None 
											},
											iothrserv = 
											contact.iothrserv match { 
											case Some(value) => Some((value - maxVector("iothrserv"))/(maxVector("iothrserv") - minVector("iothrserv"))); 
											case None => None 
											},
											iperscare = 
											contact.iperscare match { 
											case Some(value) => Some((value - maxVector("iperscare"))/(maxVector("iperscare") - minVector("iperscare"))); 
											case None => None 
											},
											iragechld = 
											contact.iragechld match { 
											case Some(value) => Some((value - maxVector("iragechld"))/(maxVector("iragechld") - minVector("iragechld"))); 
											case None => None 
											},
											irelat1 = 
											contact.irelat1 match { 
											case Some(value) => Some((value - maxVector("irelat1"))/(maxVector("irelat1") - minVector("irelat1"))); 
											case None => None 
											},
											irelat2 = 
											contact.irelat2 match { 
											case Some(value) => Some((value - maxVector("irelat2"))/(maxVector("irelat2") - minVector("irelat2"))); 
											case None => None 
											},
											iremplpar = 
											contact.iremplpar match { 
											case Some(value) => Some((value - maxVector("iremplpar"))/(maxVector("iremplpar") - minVector("iremplpar"))); 
											case None => None 
											},
											iriders = 
											contact.iriders match { 
											case Some(value) => Some((value - maxVector("iriders"))/(maxVector("iriders") - minVector("iriders"))); 
											case None => None 
											},
											irlabor = 
											contact.irlabor match { 
											case Some(value) => Some((value - maxVector("irlabor"))/(maxVector("irlabor") - minVector("irlabor"))); 
											case None => None 
											},
											irownchld = 
											contact.irownchld match { 
											case Some(value) => Some((value - maxVector("irownchld"))/(maxVector("irownchld") - minVector("irownchld"))); 
											case None => None 
											},
											irpob = 
											contact.irpob match { 
											case Some(value) => Some((value - maxVector("irpob"))/(maxVector("irpob") - minVector("irpob"))); 
											case None => None 
											},
											irrelchld = 
											contact.irrelchld match { 
											case Some(value) => Some((value - maxVector("irrelchld"))/(maxVector("irrelchld") - minVector("irrelchld"))); 
											case None => None 
											},
											irspouse = 
											contact.irspouse match { 
											case Some(value) => Some((value - maxVector("irspouse"))/(maxVector("irspouse") - minVector("irspouse"))); 
											case None => None 
											},
											irvetserv = 
											contact.irvetserv match { 
											case Some(value) => Some((value - maxVector("irvetserv"))/(maxVector("irvetserv") - minVector("irvetserv"))); 
											case None => None 
											},
											ischool = 
											contact.ischool match { 
											case Some(value) => Some((value - maxVector("ischool"))/(maxVector("ischool") - minVector("ischool"))); 
											case None => None 
											},
											isept80 = 
											contact.isept80 match { 
											case Some(value) => Some((value - maxVector("isept80"))/(maxVector("isept80") - minVector("isept80"))); 
											case None => None 
											},
											isex = 
											contact.isex match { 
											case Some(value) => Some((value - maxVector("isex"))/(maxVector("isex") - minVector("isex"))); 
											case None => None 
											},
											isubfam1 = 
											contact.isubfam1 match { 
											case Some(value) => Some((value - maxVector("isubfam1"))/(maxVector("isubfam1") - minVector("isubfam1"))); 
											case None => None 
											},
											isubfam2 = 
											contact.isubfam2 match { 
											case Some(value) => Some((value - maxVector("isubfam2"))/(maxVector("isubfam2") - minVector("isubfam2"))); 
											case None => None 
											},
											itmpabsnt = 
											contact.itmpabsnt match { 
											case Some(value) => Some((value - maxVector("itmpabsnt"))/(maxVector("itmpabsnt") - minVector("itmpabsnt"))); 
											case None => None 
											},
											ivietnam = 
											contact.ivietnam match { 
											case Some(value) => Some((value - maxVector("ivietnam"))/(maxVector("ivietnam") - minVector("ivietnam"))); 
											case None => None 
											},
											iwork89 = 
											contact.iwork89 match { 
											case Some(value) => Some((value - maxVector("iwork89"))/(maxVector("iwork89") - minVector("iwork89"))); 
											case None => None 
											},
											iworklwk = 
											contact.iworklwk match { 
											case Some(value) => Some((value - maxVector("iworklwk"))/(maxVector("iworklwk") - minVector("iworklwk"))); 
											case None => None 
											},
											iwwii = 
											contact.iwwii match { 
											case Some(value) => Some((value - maxVector("iwwii"))/(maxVector("iwwii") - minVector("iwwii"))); 
											case None => None 
											},
											iyearsch = 
											contact.iyearsch match { 
											case Some(value) => Some((value - maxVector("iyearsch"))/(maxVector("iyearsch") - minVector("iyearsch"))); 
											case None => None 
											},
											iyearwrk = 
											contact.iyearwrk match { 
											case Some(value) => Some((value - maxVector("iyearwrk"))/(maxVector("iyearwrk") - minVector("iyearwrk"))); 
											case None => None 
											}
                ).getInstanceObj
			)
		}

		def convertToContact(row : CassandraRow) : Contact= {
			new Contact(
					caseid = row.getString("caseid"),
					dage = row.getLongOption("dage") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dancstry1 = row.getLongOption("dancstry1") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dancstry2 = row.getLongOption("dancstry2") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					ddepart = row.getLongOption("ddepart") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dhispanic = row.getLongOption("dhispanic") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dhour89 = row.getLongOption("dhour89") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dhours = row.getLongOption("dhours") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dincome1 = row.getLongOption("dincome1") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dincome2 = row.getLongOption("dincome2") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dincome3 = row.getLongOption("dincome3") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dincome4 = row.getLongOption("dincome4") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dincome5 = row.getLongOption("dincome5") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dincome6 = row.getLongOption("dincome6") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dincome7 = row.getLongOption("dincome7") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dincome8 = row.getLongOption("dincome8") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dindustry = row.getLongOption("dindustry") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					doccup = row.getLongOption("doccup") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dpob = row.getLongOption("dpob") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dpoverty = row.getLongOption("dpoverty") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dpwgt1 = row.getLongOption("dpwgt1") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					drearning = row.getLongOption("drearning") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					drpincome = row.getLongOption("drpincome") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dtravtime = row.getLongOption("dtravtime") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dweek89 = row.getLongOption("dweek89") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					dyrsserv = row.getLongOption("dyrsserv") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iavail = row.getLongOption("iavail") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					icitizen = row.getLongOption("icitizen") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iclass = row.getLongOption("iclass") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					idisabl1 = row.getLongOption("idisabl1") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					idisabl2 = row.getLongOption("idisabl2") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					ienglish = row.getLongOption("ienglish") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					ifeb55 = row.getLongOption("ifeb55") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					ifertil = row.getLongOption("ifertil") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iimmigr = row.getLongOption("iimmigr") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					ikorean = row.getLongOption("ikorean") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					ilang1 = row.getLongOption("ilang1") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					ilooking = row.getLongOption("ilooking") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					imarital = row.getLongOption("imarital") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					imay75880 = row.getLongOption("imay75880") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					imeans = row.getLongOption("imeans") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					imilitary = row.getLongOption("imilitary") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					imobility = row.getLongOption("imobility") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					imobillim = row.getLongOption("imobillim") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iothrserv = row.getLongOption("iothrserv") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iperscare = row.getLongOption("iperscare") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iragechld = row.getLongOption("iragechld") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					irelat1 = row.getLongOption("irelat1") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					irelat2 = row.getLongOption("irelat2") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iremplpar = row.getLongOption("iremplpar") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iriders = row.getLongOption("iriders") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					irlabor = row.getLongOption("irlabor") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					irownchld = row.getLongOption("irownchld") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					irpob = row.getLongOption("irpob") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					irrelchld = row.getLongOption("irrelchld") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					irspouse = row.getLongOption("irspouse") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					irvetserv = row.getLongOption("irvetserv") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					ischool = row.getLongOption("ischool") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					isept80 = row.getLongOption("isept80") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					isex = row.getLongOption("isex") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					isubfam1 = row.getLongOption("isubfam1") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					isubfam2 = row.getLongOption("isubfam2") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					itmpabsnt = row.getLongOption("itmpabsnt") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					ivietnam = row.getLongOption("ivietnam") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iwork89 = row.getLongOption("iwork89") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iworklwk = row.getLongOption("iworklwk") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iwwii = row.getLongOption("iwwii") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iyearsch = row.getLongOption("iyearsch") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					},
					iyearwrk = row.getLongOption("iyearwrk") match 
					{ 
					case Some(value) => Some(DoubleDimension(value.asInstanceOf[Number].doubleValue)); 
					case None => None
					}
					)
		}
}

object ElectionDatasetReader {
	def main(args : Array[String]) {
		val conf = new SparkConf(true)
		.setAppName("GHSOM Election Dataset")
		.setMaster("local[4]")
		.set("spark.cassandra.connection.host", "localhost")
		.set("spark.cassandra.auth.username","ameya")
		.set("spark.cassandra.auth.password","amu5886")


		val sc = new SparkContext(conf)

		/*
    val maxVector = Array.fill(10)(DoubleDimension.MinValue)
    val attribVector = Array.fill(10)(DoubleDimension.getRandomDimensionValue)
    println(maxVector.mkString)
    println(attribVector.mkString)

    for ( i <- 0 until attribVector.size ) { 
        maxVector(i) = if (attribVector(i) > maxVector(i)) attribVector(i) else maxVector(i)
    } 
    println(maxVector.mkString)
		 */
		val dataset = sc.cassandraTable("uscensus1990", "dataset") 
		println("CREATING DATASET >>>>>>")
		val datasetReader : ElectionDatasetReader = new ElectionDatasetReader(dataset) 
		println("VECTOR SIZE:>>>>>>>> " )
		println(datasetReader.vectorSize)
		println("DATASET PRINT:>>>>>>>> " )
		println(datasetReader.printDataset())

	}
}
