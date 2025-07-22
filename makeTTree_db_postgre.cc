// root includes
#include "TH2.h"
#include "TH3.h"
#include "TFile.h"
#include "TString.h"
#include "TTree.h"
#include "TTreeReader.h"
#include "TTreeReaderArray.h"
#include "TTreeReaderValue.h"
#include "TChain.h"
#include "TInterpreter.h"

// sbn includes
#include "sbnanaobj/StandardRecord/StandardRecord.h"
#include "sbnanaobj/StandardRecord/SRSlice.h"
#include "sbnanaobj/StandardRecord/SRSliceRecoBranch.h"
#include "sbnanaobj/StandardRecord/SRPFP.h"

// std incldes
#include <ctime>
#include <chrono>
#include <fstream>
#include <sstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// SQL includes
#include <libpq-fe.h>
#include "sqlite3.h"

// Custom deleters for SQL ptrs
struct PGConnDeleter
{
  void operator()(PGconn* conn) const
  {
    if (conn) PQfinish(conn);
  }
};
struct PGResultDeleter
{
  void operator()(PGresult* res) const
  {
    if (res) PQclear(res);
  }
};
struct sqlite3Deleter
{
  void operator()(sqlite3* db) const
  {
    sqlite3_close(db);
  }
};
struct stmtDeleter
{
  void operator()(sqlite3_stmt* stmt) const
  {
    sqlite3_finalize(stmt);
  }
};

// make a stuct for reading the runinfo
struct runInfo
{
  bool epicsExists;
  int Run;
  int Start;
  int End;
  double Cath;
  double EInd1;
  double EInd2;
  double EColl;
  double WInd1;
  double WInd2;
  double WColl;
  bool confgExists;
  TString Config;
  int NTPC;
  int NPMT;
  int NCRT;
  runInfo(int run)
  {
    Run = run;
    epicsExists = false;
    confgExists = false;
    std::unique_ptr<sqlite3, sqlite3Deleter> runInfoDB;
    sqlite3* rawDB;
    //if(sqlite3_open("icarus_metadata.db", &rawDB) != SQLITE_OK)
    if(sqlite3_open("", &rawDB) != SQLITE_OK)
    {
      std::cerr << "Could not open icarus_metadata.db:" << '\n'
                << sqlite3_errmsg(runInfoDB.get()) << '\n'
                << "bail." << std::endl;
                return;
    }
    runInfoDB.reset(rawDB);
    std::unique_ptr<sqlite3_stmt, stmtDeleter> runStmt;
    sqlite3_stmt* rawStmt;
    std::string runStmtStr = "SELECT * FROM runinfo WHERE run_number=" + std::to_string(run) + " ;";
    if (sqlite3_prepare_v2(runInfoDB.get(),
                           runStmtStr.c_str(),
                           runStmtStr.length(),
                           &rawStmt,
                           nullptr             ) != SQLITE_OK)
    {
      std::cerr << "Could not prepare statement 'SELECT * FROM runinfo WHERE run_number=" + std::to_string(run) + "'"
                << '\n' << "bail." << std::endl;
      return;
    }
    runStmt.reset(rawStmt);
    if (sqlite3_step(runStmt.get()) != SQLITE_ROW)
    {
      std::cerr << "Could not step statement 'SELECT * FROM runinfo WHERE run_number=" + std::to_string(run) + "'"
                << '\n' << "bail." << std::endl;
    } else
    {
      epicsExists = true;
      WColl = sqlite3_column_double(runStmt.get(), 10);
    }
    std::ifstream confgStrm("configurations.csv");
    if (confgStrm.good())
    {
      std::string confgLine;
      while (std::getline(confgStrm, confgLine))
      {
        std::stringstream confgLineStrm(confgLine);
        std::string tmpStr;
        std::vector<std::string> tmpVals;
        while (std::getline(confgLineStrm, tmpStr, ','))
          tmpVals.push_back(tmpStr);
        if (tmpVals.front() == std::to_string(run))
        {
          confgExists = true;
          Config =           tmpVals[1];
          NTPC   = std::stoi(tmpVals[2]);
          NPMT   = std::stoi(tmpVals[3]);
          NCRT   = std::stoi(tmpVals[4]);
          break;
        }
      }
    }
  }
};

// format a time from a TString
std::time_t makeTime(TString timeStr, bool debug = false)
{
  // initialize return
  std::tm parsedTime{};

  // timeStr should be indexed as
  // [ 0 -  2] Day of the week
  // [ 4 -  6] Month
  // [ 8 -  9] Day
  // [11 - 12] Hour
  // [14 - 15] Minute
  // [17 - 18] Second
  // [20 - 22] Timezone
  // [24 - 27] Year
  if (debug) std::cout << "Parsing time from string " << timeStr.Data() << std::endl;
  TString secStr(timeStr(17, 2));
  int sec = secStr.Atoi();
  if (debug) std::cout << "  From " << '"' << secStr.Data() << '"' << " we get " << sec << std::endl;
  TString minStr(timeStr(14, 2));
  int min = minStr.Atoi();
  if (debug) std::cout << "  From " << '"' << minStr.Data() << '"' << " we get " << min << std::endl;
  TString hourStr(timeStr(11, 2));
  int hour = hourStr.Atoi();
  if (debug) std::cout << "  From " << '"' << hourStr.Data() << '"' << " we get " << hour << std::endl;
  TString dayStr(timeStr(8, 2));
  int day = dayStr.Atoi();
  if (debug) std::cout << "  From " << '"' << dayStr.Data() << '"' << " we get " << day << std::endl;
  TString monthStr(timeStr(4, 3));
  int month = (monthStr.EqualTo("Jan")) ?  0 :
              (monthStr.EqualTo("Feb")) ?  1 :
              (monthStr.EqualTo("Mar")) ?  2 :
              (monthStr.EqualTo("Apr")) ?  3 :
              (monthStr.EqualTo("May")) ?  4 :
              (monthStr.EqualTo("Jun")) ?  5 :
              (monthStr.EqualTo("Jul")) ?  6 :
              (monthStr.EqualTo("Aug")) ?  7 :
              (monthStr.EqualTo("Sep")) ?  8 :
              (monthStr.EqualTo("Oct")) ?  9 :
              (monthStr.EqualTo("Nov")) ? 10 :
              (monthStr.EqualTo("Dec")) ? 11 :
                                          12 ;
  if (debug) std::cout << "  From " << '"' << monthStr.Data() << '"' << " we get " << month << std::endl;
  TString yearStr(timeStr(24, 4));
  int year = yearStr.Atoi();
  if (debug) std::cout << "  From " << '"' << yearStr.Data() << '"' << " we get " << year << std::endl;
  TString wdStr(timeStr(0, 3));
  int weekday = (wdStr.EqualTo("Sun")) ? 0 :
                (wdStr.EqualTo("Mon")) ? 1 :
                (wdStr.EqualTo("Tue")) ? 2 :
                (wdStr.EqualTo("Wed")) ? 3 :
                (wdStr.EqualTo("Thu")) ? 4 :
                (wdStr.EqualTo("Fri")) ? 5 :
                (wdStr.EqualTo("Sat")) ? 6 :
                                         7 ;
  if (debug) std::cout << "  From " << '"' << wdStr.Data() << '"' << " we get " << weekday << std::endl;
  TString dlStr(timeStr(20, 3));
  int daylight = (dlStr.EqualTo("CDT")) ? 1 :
                 (dlStr.EqualTo("CST")) ? 0 :
                                         -1 ;
  if (debug) std::cout << "  From " << '"' << dlStr.Data() << '"' << " we get " << daylight << std::endl;

  parsedTime.tm_sec = sec;
  parsedTime.tm_min = min;
  parsedTime.tm_hour = hour;
  parsedTime.tm_mday = day;
  parsedTime.tm_mon = month;
  parsedTime.tm_year = year - 1900;
  parsedTime.tm_wday = weekday;
  parsedTime.tm_isdst = daylight;
  std::time_t timePoint = std::mktime(&parsedTime);

  if (month == 12 || weekday == 7 || daylight == -1)
  {
    std::string badInput(timeStr.Data());
    std::cerr << "Error, invalid date string " << badInput << '\n'
              << "  sec " << sec << '\n'
              << "  min " << min << '\n'
              << "  hour " << hour << '\n'
              << "  day " << day << '\n'
              << "  month " << '\n'
              << "  year " << year << '\n'
              << "  weekday " << weekday << '\n'
              << "  daylight " << daylight << std::endl;
    return std::mktime(&parsedTime);
  }

  return timePoint;
}

void makeTTree_db_postgre(std::string srFileName, std::string outFileName, bool debug = false)
{
  // get the weights stored correctly
  TH1::SetDefaultSumw2(true);
  gInterpreter->GenerateDictionary("vector<vector<float>>", "vector");
  gInterpreter->GenerateDictionary("vector<vector<int>>", "vector");


  // open the trigger database
  /*  std::string trgStmtStr = "SELECT * FROM triggerdata WHERE run_number=";
  std::string trgParams  = "dbname=icarus_trigger_prd ";
              trgParams += "user=triggerdb_reader ";
              trgParams += "password=read4trigdb ";
              trgParams += "host=ifdbdaqrep01.fnal.gov ";
              trgParams += "port=5455";
  std::unique_ptr<PGconn, PGConnDeleter> dbConnection(PQconnectdb(trgParams.c_str()));
  if (PQstatus(dbConnection.get()) != CONNECTION_OK)
  {
    std::cerr << "Connection to trigger datatbase failed" << std::endl;
    PQfinish(dbConnection.get());
    return;
    }*/
  //make tchain lists instead of looping over individual files

  //TFile* srFile;
  TChain* srTree=new TChain("recTree");
  if(srFileName.find(".txt")!=std::string::npos){//contains substring
    std::ifstream infileList(srFileName);
    string infile;
    while (std::getline(infileList, infile)){
      cout<<"infile: "<<infile<<endl;
      srTree->Add(infile.c_str() );
    }
  }
  else{//single root file input
    
    // open the file and get the StandardRecord TTree
    //std::unique_ptr<TFile> srFile(TFile::Open(srFileName.c_str(), "READ"));
    /*srFile=new TFile(srFileName.c_str(), "READ");
    if (not srFile){
      std::cout << "Could not open file. Bail." << std::endl;
      return;
      }*/

    // TTreeReaderArrays can't handle multi-dimensional arrays, so we have to do it the old fasioned way
    /*if (not srFile->GetListOfKeys()->FindObject("recTree"))
      {
        std::cout << "Could not find recTree in file. Bail." << std::endl;
        return;
        }*/
    srTree->Add(srFileName.c_str() );
    //std::unique_ptr<TTree> srTree(srFile->Get<TTree>("recTree"));
    if (not srTree)
      {
        std::cout << "Could not open recTree. Bail." << std::endl;
        return;
      }
  }
  // Open an output file
  // make a TTree to fill
  std::unique_ptr<TFile> outFile = std::make_unique<TFile>(outFileName.c_str(), "RECREATE");
  outFile->cd();
  std::unique_ptr<TTree> outTree = std::make_unique<TTree>("data_validation_tree", "Data Validation Tree");

  // loop to get how big each slice-pfp "array" should be
  std::vector<size_t> sizeSliceArr(srTree->GetEntries(), 0);
  std::vector<size_t> sizePFPArr(srTree->GetEntries(), 0);
  std::vector<size_t> sizeOpFlashArr(srTree->GetEntries(), 0);
  std::vector<size_t> sizeCRTHitArr(srTree->GetEntries(), 0);
  std::vector<size_t> sizeCRTTrackArr(srTree->GetEntries(), 0);
  std::vector<size_t> sizeCRTPMTMatchArr(srTree->GetEntries(), 0);
  std::vector<size_t> sizeCRTPMTMatchHitArr(srTree->GetEntries(), 0);
  for (size_t evt = 0; evt < srTree->GetEntries(); ++evt)
  {
    int srbNSlices;
    srTree->SetBranchAddress("rec.nslc", &srbNSlices);
    int srbNOpFlashes;
    srTree->SetBranchAddress("rec.nopflashes", &srbNOpFlashes);
    int srbNCRTHits;
    srTree->SetBranchAddress("rec.ncrt_hits", &srbNCRTHits);
    int srbNCRTTracks;
    srTree->SetBranchAddress("rec.ncrt_tracks", &srbNCRTTracks);
    int srbNCRTPMTMatches;
    srTree->SetBranchAddress("rec.ncrtpmt_matches", &srbNCRTPMTMatches);
    srTree->GetEntry(evt);
    sizeSliceArr[evt] = srbNSlices;
    sizeOpFlashArr[evt] = srbNOpFlashes;
    sizeCRTHitArr[evt] = srbNCRTHits;
    sizeCRTTrackArr[evt] = srbNCRTTracks;
    sizeCRTPMTMatchArr[evt] = srbNCRTPMTMatches;
    std::unique_ptr<ULong64_t[]> srbNPFPinSlice = std::make_unique<ULong64_t[]>(sizeSliceArr[evt]);
    srTree->SetBranchAddress("rec.slc.reco.npfp", srbNPFPinSlice.get());
    std::unique_ptr<int[]> srbNHitInMatch = std::make_unique<int[]>(srbNCRTPMTMatches);
    srTree->SetBranchAddress("rec.crtpmt_matches.matchedCRTHits..length", srbNHitInMatch.get());
    srTree->GetEntry(evt);
    for (size_t slc = 0; slc < sizeSliceArr[evt]; ++slc)
    {
      sizePFPArr[evt] += srbNPFPinSlice[slc];
    }
    for (size_t match = 0; match < sizeCRTPMTMatchArr[evt]; ++match)
    {
      sizeCRTPMTMatchHitArr[evt] += srbNHitInMatch[match];
    }
    srTree->ResetBranchAddresses();
  }

  // declare our new branches
  unsigned int newRun;
  unsigned int newSubrun;
  unsigned int newEvent;
  float newPOT;
  // TPC
  int newNSlices;
  std::vector<unsigned long long> newNPFPinSlice;
  std::vector<char> newClearCosmic;
  std::vector<float> newCRLongestTrackDirY;
  std::vector<bool> newFMatchPresent;
  std::vector<float> newFMatchLightPE;
  std::vector<float> newFMatchScore;
  std::vector<std::vector<float>> newTrackLength;
  std::vector<std::vector<float>> newShowerLength;
  std::vector<std::vector<int>> newTrackBestPlane;
  std::vector<std::vector<int>> newShowerBestPlane;
  std::vector<std::vector<float>> newTrackDirY;
  std::vector<std::vector<float>> newTrackVtxX;
  std::vector<std::vector<float>> newTrackVtxY;
  std::vector<std::vector<float>> newTrackVtxZ;
  std::vector<std::vector<int>> newTrackNHit1;
  std::vector<std::vector<int>> newTrackNHit2;
  std::vector<std::vector<int>> newTrackNHit3;
  // PMT
  int newNOpFlashes;
  std::vector<float> newFlashTimeWidth;
  std::vector<float> newFlashTimeSD;
  std::vector<float> newFlashTotalPE;
  // CRT
  int newNCRTHits;
  std::vector<int> newCRTHitPlane;
  std::vector<float> newCRTHitPE;
  std::vector<float> newCRTHitErrX;
  std::vector<float> newCRTHitErrY;
  std::vector<float> newCRTHitErrZ;
  int newNCRTTracks;
  std::vector<float> newCRTTrackTime;
  int newNCRTPMTMatches;
  std::vector<int> newNMatchedCRTPMTHits;
  std::vector<std::vector<double>> newMatchedCRTPMTimeDiff;
  // DB
  bool             dbRunInfoExists;
  bool             dbTriggerDataExists;
  int              dbRun;
  int              dbStart;
  int              dbEnd;
  TString          dbConfig;
  double           dbCath;
  double           dbEInd1;
  double           dbEInd2;
  double           dbEColl;
  double           dbWInd1;
  double           dbWInd2;
  double           dbWColl;
  int              dbNTPC;
  int              dbNPMT;
  int              dbNCRT;
  int              dbEventNumber;
  int              dbTrigSec;
  int              dbTrigNanosec;
  int              dbGateType;
  int              dbTrigSource;

  // set up output branches for each variable
  outTree->Branch("run", &newRun);
  outTree->Branch("subrun", &newSubrun);
  outTree->Branch("event", &newEvent);
  outTree->Branch("POT", &newPOT);
  outTree->Branch("nslc", &newNSlices);
  outTree->Branch("slc.npfp", &newNPFPinSlice);
  outTree->Branch("slc.clear_cosmic", &newClearCosmic);
  outTree->Branch("slc.CRLongestTrackDirY", &newCRLongestTrackDirY);
  outTree->Branch("slc.FMatchPresent", &newFMatchPresent);
  outTree->Branch("slc.FMatchLightPE", &newFMatchLightPE);
  outTree->Branch("slc.FMatchScore", &newFMatchScore);
  outTree->Branch("slc.pfp.trackLength", &newTrackLength);
  outTree->Branch("slc.pfp.showerLength", &newShowerLength);
  outTree->Branch("slc.pfp.trackBestPlane", &newTrackBestPlane);
  outTree->Branch("slc.pfp.showerBestPlane", &newShowerBestPlane);
  outTree->Branch("slc.pfp.trackDirY", &newTrackDirY);
  outTree->Branch("slc.pfp.trackVtxX", &newTrackVtxX);
  outTree->Branch("slc.pfp.trackVtxY", &newTrackVtxY);
  outTree->Branch("slc.pfp.trackVtxZ", &newTrackVtxZ);
  outTree->Branch("slc.pfp.trackNHit1", &newTrackNHit1);
  outTree->Branch("slc.pfp.trackNHit2", &newTrackNHit2);
  outTree->Branch("slc.pfp.trackNHit3", &newTrackNHit3);
  outTree->Branch("nflash", &newNOpFlashes);
  outTree->Branch("flash.timeWidth", &newFlashTimeWidth);
  outTree->Branch("flash.timeSD", &newFlashTimeSD);
  outTree->Branch("flash.PE", &newFlashTotalPE);
  outTree->Branch("ncrthit", &newNCRTHits);
  outTree->Branch("crt_hit.plane", &newCRTHitPlane);
  outTree->Branch("crt_hit.PE", &newCRTHitPE);
  outTree->Branch("crt_hit.err_x", &newCRTHitErrX);
  outTree->Branch("crt_hit.err_y", &newCRTHitErrY);
  outTree->Branch("crt_hit.err_z", &newCRTHitErrZ);
  outTree->Branch("ncrt_track", &newNCRTTracks);
  outTree->Branch("crt_track.time", &newCRTTrackTime);
  outTree->Branch("ncrtpmt_match", &newNCRTPMTMatches);
  outTree->Branch("crtpmt_match.nhit", &newNMatchedCRTPMTHits);
  outTree->Branch("crtpmt_match.hit.timeDiff", &newMatchedCRTPMTimeDiff);
  outTree->Branch("db.runinfo_exists", &dbRunInfoExists);
  outTree->Branch("db.triggerdata_exists", &dbTriggerDataExists);
  outTree->Branch("db.run", &dbRun);
  outTree->Branch("db.start", &dbStart);
  outTree->Branch("db.end", &dbEnd);
  outTree->Branch("db.config", &dbConfig);
  outTree->Branch("db.cathodeV", &dbCath);
  outTree->Branch("db.EInd1V", &dbEInd1);
  outTree->Branch("db.EInd2V", &dbEInd2);
  outTree->Branch("db.ECollV", &dbEColl);
  outTree->Branch("db.WInd1V", &dbWInd1);
  outTree->Branch("db.WInd2V", &dbWInd2);
  outTree->Branch("db.WCollV", &dbWColl);
  outTree->Branch("db.NTPC", &dbNTPC);
  outTree->Branch("db.NPMT", &dbNPMT);
  outTree->Branch("db.NCRT", &dbNCRT);
  outTree->Branch("db.event", &dbEventNumber);
  outTree->Branch("db.trigSec", &dbTrigSec);
  outTree->Branch("db.trigNanosec", &dbTrigNanosec);
  outTree->Branch("db.gateType", &dbGateType);
  outTree->Branch("db.trigSource", &dbTrigSource);

  // loop over events
  for (size_t evt = 0; evt < srTree->GetEntries(); ++evt)
  {
    // set up the vars
    unsigned int srbRun;
    srTree->SetBranchAddress("rec.hdr.run", &srbRun);
    unsigned int srbSubrun;
    srTree->SetBranchAddress("rec.hdr.subrun", &srbSubrun);
    unsigned int srbEvent;
    srTree->SetBranchAddress("rec.hdr.evt", &srbEvent);
    int srbNSlices;
    srTree->SetBranchAddress("rec.nslc", &srbNSlices);
    std::unique_ptr<ULong64_t[]> srbNPFPinSlice = std::make_unique<ULong64_t[]>(sizeSliceArr[evt]);
    srTree->SetBranchAddress("rec.slc.reco.npfp", srbNPFPinSlice.get());
    std::unique_ptr<Char_t[]> srbClearCosmic = std::make_unique<Char_t[]>(sizeSliceArr[evt]);
    srTree->SetBranchAddress("rec.slc.is_clear_cosmic", srbClearCosmic.get());
    std::unique_ptr<float[]> srbCRLongestTrackDirY = std::make_unique<float[]>(sizeSliceArr[evt]);
    srTree->SetBranchAddress("rec.slc.nuid.crlongtrkdiry", srbCRLongestTrackDirY.get());
    std::string recoPFPStr = "rec.slc.reco.pfp";
    std::unique_ptr<float[]> srbTrackLength = std::make_unique<float[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".trk.len").c_str(), srbTrackLength.get());
    std::unique_ptr<float[]> srbShowerLength = std::make_unique<float[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".shw.len").c_str(), srbShowerLength.get());
    std::unique_ptr<caf::Plane_t[]> srbTrackBestPlane = std::make_unique<caf::Plane_t[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".trk.bestplane").c_str(), srbTrackBestPlane.get());
    std::unique_ptr<int[]> srbShowerBestPlane = std::make_unique<int[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".shw.bestplane").c_str(), srbShowerBestPlane.get());
    std::unique_ptr<float[]> srbTrackDirY = std::make_unique<float[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".trk.dir.y").c_str(), srbTrackDirY.get());
    std::unique_ptr<float[]> srbTrackVtxX = std::make_unique<float[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".trk.start.x").c_str(), srbTrackVtxX.get());
    std::unique_ptr<float[]> srbTrackVtxY = std::make_unique<float[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".trk.start.y").c_str(), srbTrackVtxY.get());
    std::unique_ptr<float[]> srbTrackVtxZ = std::make_unique<float[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".trk.start.z").c_str(), srbTrackVtxZ.get());
    std::unique_ptr<int[]> srbTrackNHit1 = std::make_unique<int[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".trk.calo.0.nhit").c_str(), srbTrackNHit1.get());
    std::unique_ptr<int[]> srbTrackNHit2 = std::make_unique<int[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".trk.calo.1.nhit").c_str(), srbTrackNHit2.get());
    std::unique_ptr<int[]> srbTrackNHit3 = std::make_unique<int[]>(sizePFPArr[evt]);
    srTree->SetBranchAddress((recoPFPStr+".trk.calo.2.nhit").c_str(), srbTrackNHit3.get());
    int srbNOpFlashes;
    srTree->SetBranchAddress("rec.nopflashes", &srbNOpFlashes);
    std::unique_ptr<float[]> srbFlashTimeWidth = std::make_unique<float[]>(sizeOpFlashArr[evt]);
    srTree->SetBranchAddress("rec.opflashes.timewidth", srbFlashTimeWidth.get());
    std::unique_ptr<float[]> srbFlashTimeSD = std::make_unique<float[]>(sizeOpFlashArr[evt]);
    srTree->SetBranchAddress("rec.opflashes.timesd", srbFlashTimeSD.get());
    std::unique_ptr<float[]> srbFlashTotalPE = std::make_unique<float[]>(sizeOpFlashArr[evt]);
    srTree->SetBranchAddress("rec.opflashes.totalpe", srbFlashTotalPE.get());
    int srbNCRTHits;
    srTree->SetBranchAddress("rec.ncrt_hits", &srbNCRTHits);
    std::unique_ptr<int[]> srbCRTHitPlane = std::make_unique<int[]>(sizeCRTHitArr[evt]);
    srTree->SetBranchAddress("rec.crt_hits.plane", srbCRTHitPlane.get());
    std::unique_ptr<float[]> srbCRTHitPE = std::make_unique<float[]>(sizeCRTHitArr[evt]);
    srTree->SetBranchAddress("rec.crt_hits.pe", srbCRTHitPE.get());
    std::unique_ptr<float[]> srbCRTHitErrX = std::make_unique<float[]>(sizeCRTHitArr[evt]);
    srTree->SetBranchAddress("rec.crt_hits.position_err.x", srbCRTHitErrX.get());
    std::unique_ptr<float[]> srbCRTHitErrY = std::make_unique<float[]>(sizeCRTHitArr[evt]);
    srTree->SetBranchAddress("rec.crt_hits.position_err.y", srbCRTHitErrY.get());
    std::unique_ptr<float[]> srbCRTHitErrZ = std::make_unique<float[]>(sizeCRTHitArr[evt]);
    srTree->SetBranchAddress("rec.crt_hits.position_err.z", srbCRTHitErrZ.get());
    int srbNCRTTracks;
    srTree->SetBranchAddress("rec.ncrt_tracks", &srbNCRTTracks);
    std::unique_ptr<float[]> srbCRTTrackTime = std::make_unique<float[]>(sizeCRTTrackArr[evt]);
    srTree->SetBranchAddress("rec.crt_tracks.time", srbCRTTrackTime.get());
    int srbNCRTPMTMatches;
    srTree->SetBranchAddress("rec.ncrtpmt_matches", &srbNCRTPMTMatches);
    std::unique_ptr<int[]> srbNMatchedCRTPMTHits = std::make_unique<int[]>(sizeCRTPMTMatchArr[evt]);
    srTree->SetBranchAddress("rec.crtpmt_matches.matchedCRTHits..length", srbNMatchedCRTPMTHits.get());
    std::unique_ptr<double[]> srbMatchedCRTPMTimeDiff = std::make_unique<double[]>(sizeCRTPMTMatchHitArr[evt]);
    srTree->SetBranchAddress("rec.crtpmt_matches.matchedCRTHits.PMTTimeDiff", srbMatchedCRTPMTimeDiff.get());

    // get the entry
    srTree->GetEntry(evt);

    // get info from DB
    //runInfo dbRunInfo(srbRun);
    /*dbTriggerDataExists = true;
    std::string trgStmtStrFull = trgStmtStr + std::to_string(srbRun)
                               + " AND event_no=" + std::to_string(srbEvent) + "; ";
    std::unique_ptr<PGresult, PGResultDeleter> trgRes(PQexec(dbConnection.get() ,trgStmtStrFull.c_str()));
    if (PQresultStatus(trgRes.get()) != PGRES_TUPLES_OK)
    {
      std::cerr << "Statement '" << trgStmtStrFull << "' failed" << std::endl;
      dbTriggerDataExists = false;
    } else {
      std::string queryMin = trgStmtStr + std::to_string(srbRun) + " ORDER BY seconds ASC LIMIT 1 ; ";
      std::string queryMax = trgStmtStr + std::to_string(srbRun) + " ORDER BY seconds DESC LIMIT 1 ; ";
      std::unique_ptr<PGresult, PGResultDeleter> secMin(PQexec(dbConnection.get() ,queryMin.c_str()));
      std::unique_ptr<PGresult, PGResultDeleter> secMax(PQexec(dbConnection.get() ,queryMax.c_str()));
      dbStart = std::stol(PQgetvalue(secMin.get(), 0, 3));
      dbEnd   = std::stol(PQgetvalue(secMax.get(), 0, 3));
    }
    dbRun              = (dbRunInfo.epicsExists) ? dbRunInfo.Run                              : -1;
    dbStart            = (dbTriggerDataExists)   ? dbStart                                    : -1;
    dbEnd              = (dbTriggerDataExists)   ? dbEnd                                      : -1;
    dbConfig           = (dbRunInfo.confgExists) ? dbRunInfo.Config                           : "";
    dbCath             = (dbRunInfo.epicsExists) ? dbRunInfo.Cath                             : 0;
    dbEInd1            = (dbRunInfo.epicsExists) ? dbRunInfo.EInd1                            : 0;
    dbEInd2            = (dbRunInfo.epicsExists) ? dbRunInfo.EInd2                            : 0;
    dbEColl            = (dbRunInfo.epicsExists) ? dbRunInfo.EColl                            : 0;
    dbWInd1            = (dbRunInfo.epicsExists) ? dbRunInfo.WInd1                            : 0;
    dbWInd2            = (dbRunInfo.epicsExists) ? dbRunInfo.WInd2                            : 0;
    dbWColl            = (dbRunInfo.epicsExists) ? dbRunInfo.WColl                            : 0;
    dbNTPC             = (dbRunInfo.confgExists) ? dbRunInfo.NTPC                             : -1;
    dbNPMT             = (dbRunInfo.confgExists) ? dbRunInfo.NPMT                             : -1;
    dbNCRT             = (dbRunInfo.confgExists) ? dbRunInfo.NCRT                             : -1;
    dbEventNumber      = (dbTriggerDataExists)   ? std::stol(PQgetvalue(trgRes.get(), 0,  2)) : -1;
    dbTrigSec          = (dbTriggerDataExists)   ? std::stol(PQgetvalue(trgRes.get(), 0,  3)) : -1;
    dbTrigNanosec      = (dbTriggerDataExists)   ? std::stol(PQgetvalue(trgRes.get(), 0,  4)) : -1;
    dbGateType         = (dbTriggerDataExists)   ? std::stol(PQgetvalue(trgRes.get(), 0, 13)) : -1;
    dbTrigSource       = (dbTriggerDataExists)   ? std::stol(PQgetvalue(trgRes.get(), 0, 20)) : -1;
    if (debug)
      std::cout << "~~~From CSVs ~~~" << '\n'
                << "   Run "                      << dbRun           << '\n'
                << "   Started (global trigger) " << dbStart         << '\n'
                << "   Ended (global trigger) "   << dbEnd           << '\n'
                << "   Cathode at "               << dbCath  << " V" << '\n'
                << "   East Ind1 Wire Bias at "   << dbEInd1 << " V" << '\n'
                << "   East Ind2 Wire Bias at "   << dbEInd2 << " V" << '\n'
                << "   East Coll Wire Bias at "   << dbEColl << " V" << '\n'
                << "   West Ind1 Wire Bias at "   << dbWInd1 << " V" << '\n'
                << "   West Ind2 Wire Bias at "   << dbWInd2 << " V" << '\n'
                << "   West Coll Wire Bias at "   << dbWColl << " V" << '\n'
                << "   Configuration "            << dbConfig        << '\n'
                << "   TPC Components:  "         << dbNTPC          << '\n'
                << "   PMT Components:  "         << dbNPMT          << '\n'
                << "   CRT Components:  "         << dbNCRT          << '\n'
                << "~~~From DataBase using statement << " << trgStmtStrFull << "~~~" << '\n'
                << "   Event "                    << dbEventNumber   << '\n'
                << "   Seconds "                  << dbTrigSec       << '\n'
                << "   Nanoseconds "              << dbTrigNanosec   << '\n'
                << "   Gate Type "                << dbGateType      << '\n'
                << "   Source "                   << dbTrigSource    << std::endl;
    */            
    // set the event here
    newRun = srbRun;
    newSubrun = srbSubrun;
    newEvent = srbEvent;
    // TPC
    newNSlices = srbNSlices;
    newNPFPinSlice.clear();
    newClearCosmic.clear();
    newCRLongestTrackDirY.clear();
    newTrackLength.clear();
    newShowerLength.clear();
    newTrackBestPlane.clear();
    newShowerBestPlane.clear();
    newTrackDirY.clear();
    newTrackVtxX.clear();
    newTrackVtxY.clear();
    newTrackVtxZ.clear();
    newTrackNHit1.clear();
    newTrackNHit2.clear();
    newTrackNHit3.clear();
    // PMT
    newNOpFlashes = srbNOpFlashes;
    newFlashTimeWidth.clear();
    newFlashTimeSD.clear();
    newFlashTotalPE.clear();
    // CRT
    newNCRTHits = srbNCRTHits;
    newCRTHitPlane.clear();
    newCRTHitPE.clear();
    newCRTHitErrX.clear();
    newCRTHitErrY.clear();
    newCRTHitErrZ.clear();
    newNCRTTracks = srbNCRTTracks;
    newCRTTrackTime.clear();
    newNCRTPMTMatches = srbNCRTPMTMatches;
    newNMatchedCRTPMTHits.clear();
    newMatchedCRTPMTimeDiff.clear();

    // Fill TPC info
    std::vector<float> tempTrackLength;
    std::vector<float> tempShowerLength;
    std::vector<int> tempTrackBestPlane;
    std::vector<int> tempShowerBestPlane;
    std::vector<float> tempTrackDirY;
    std::vector<float> tempTrackVtxX;
    std::vector<float> tempTrackVtxY;
    std::vector<float> tempTrackVtxZ;
    std::vector<int> tempTrackNHit1;
    std::vector<int> tempTrackNHit2;
    std::vector<int> tempTrackNHit3;
    size_t srbSlicePFPIdx = 0;
    for (size_t slc_idx = 0; slc_idx < srbNSlices; ++slc_idx)
    {
      newNPFPinSlice.emplace_back(srbNPFPinSlice[slc_idx]);
      newClearCosmic.emplace_back(srbClearCosmic[slc_idx]);
      if (debug) std::cout << "Add ClearCosmic " << srbClearCosmic[slc_idx] << std::endl;
      newCRLongestTrackDirY.emplace_back(srbCRLongestTrackDirY[slc_idx]);
      if (debug) std::cout << "Add CRLongestTrackDirY " << srbCRLongestTrackDirY[slc_idx] << std::endl;
      tempTrackLength.clear();
      tempShowerLength.clear();
      tempTrackBestPlane.clear();
      tempShowerBestPlane.clear();
      tempTrackDirY.clear();
      tempTrackVtxX.clear();
      tempTrackVtxY.clear();
      tempTrackVtxZ.clear();
      tempTrackNHit1.clear();
      tempTrackNHit2.clear();
      tempTrackNHit3.clear();
      for (size_t pfp_idx = 0; pfp_idx < srbNPFPinSlice[slc_idx]; ++pfp_idx)
      {
        tempTrackLength.emplace_back(srbTrackLength[srbSlicePFPIdx]);
        tempShowerLength.emplace_back(srbShowerLength[srbSlicePFPIdx]);
        tempTrackBestPlane.emplace_back(srbTrackBestPlane[srbSlicePFPIdx]);
        tempShowerBestPlane.emplace_back(srbShowerBestPlane[srbSlicePFPIdx]);
        tempTrackDirY.emplace_back(srbTrackDirY[srbSlicePFPIdx]);
        tempTrackVtxX.emplace_back(srbTrackVtxX[srbSlicePFPIdx]);
        tempTrackVtxY.emplace_back(srbTrackVtxY[srbSlicePFPIdx]);
        tempTrackVtxZ.emplace_back(srbTrackVtxZ[srbSlicePFPIdx]);
        tempTrackNHit1.emplace_back(srbTrackNHit1[srbSlicePFPIdx]);
        tempTrackNHit2.emplace_back(srbTrackNHit2[srbSlicePFPIdx]);
        tempTrackNHit3.emplace_back(srbTrackNHit3[srbSlicePFPIdx]);
        ++srbSlicePFPIdx;
      }
      newTrackLength.emplace_back(tempTrackLength);
      newShowerLength.emplace_back(tempShowerLength);
      newTrackBestPlane.emplace_back(tempTrackBestPlane);
      newShowerBestPlane.emplace_back(tempShowerBestPlane);
      newTrackDirY.emplace_back(tempTrackDirY);
      newTrackVtxX.emplace_back(tempTrackVtxX);
      newTrackVtxY.emplace_back(tempTrackVtxY);
      newTrackVtxZ.emplace_back(tempTrackVtxZ);
      newTrackNHit1.emplace_back(tempTrackNHit1);
      newTrackNHit2.emplace_back(tempTrackNHit2);
      newTrackNHit3.emplace_back(tempTrackNHit3);
    }
    // Fill PMT info
    for (size_t flsh_idx = 0; flsh_idx < srbNOpFlashes; ++flsh_idx)
    {
      newFlashTimeWidth.emplace_back(srbFlashTimeWidth[flsh_idx]);
      newFlashTimeSD.emplace_back(srbFlashTimeSD[flsh_idx]);
      newFlashTotalPE.emplace_back(srbFlashTotalPE[flsh_idx]);
    }
    // Fill CRT info
    // (hits)
    for (size_t crt_hit_idx = 0; crt_hit_idx < srbNCRTHits; ++crt_hit_idx)
    {  
      newCRTHitPlane.emplace_back(srbCRTHitPlane[crt_hit_idx]);
      newCRTHitPE.emplace_back(srbCRTHitPE[crt_hit_idx]);
      newCRTHitErrX.emplace_back(srbCRTHitErrX[crt_hit_idx]);
      newCRTHitErrY.emplace_back(srbCRTHitErrY[crt_hit_idx]);
      newCRTHitErrZ.emplace_back(srbCRTHitErrZ[crt_hit_idx]);
    }
    // (tracks)
    for (size_t crt_trk_idx = 0; crt_trk_idx < srbNCRTTracks; ++crt_trk_idx)
    {
      newCRTTrackTime.emplace_back(srbCRTTrackTime[crt_trk_idx]);
    }
    // (matches)
    std::vector<double> tempMatchedCRTPMTimeDiff;
    size_t srbMatchHitIdx = 0;
    for (size_t crt_mtch_idx = 0; crt_mtch_idx < srbNCRTPMTMatches; ++crt_mtch_idx)
    {
      newNMatchedCRTPMTHits.emplace_back(srbNMatchedCRTPMTHits[crt_mtch_idx]);
      tempMatchedCRTPMTimeDiff.clear();
      for (size_t crt_mtch_hit_idx = 0; crt_mtch_hit_idx < srbNMatchedCRTPMTHits[crt_mtch_idx]; ++crt_mtch_hit_idx)
      {
        tempMatchedCRTPMTimeDiff.emplace_back(srbMatchedCRTPMTimeDiff[srbMatchHitIdx]);
        ++srbMatchHitIdx;
      }
      newMatchedCRTPMTimeDiff.emplace_back(tempMatchedCRTPMTimeDiff);
    }

    // fill new TTree
    outTree->Fill();

    // we have to reset the branch addresses before we clean up
    srTree->ResetBranchAddresses();
  }

  if (debug)
    std::cout << "...writing..." << std::endl;
  outFile->Write();
  if (debug)
    outTree->Print();
  if (debug)
    std::cout << "...release ptrs owned by TFiles..." << std::endl;
  //srTree->release();
  delete srTree;
  outTree.release();
  if (debug)
    std::cout << "...closing..." << std::endl;
  //if(srFile) srFile->Close();
  outFile->Close();
  if (debug)
    std::cout << "...done!" << std::endl;
}
