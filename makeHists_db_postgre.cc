// root includes
#include "TH2.h"
#include "TH3.h"
#include "TFile.h"
#include "TString.h"
#include "TTree.h"
#include "TTreeReader.h"
#include "TTreeReaderArray.h"
#include "TTreeReaderValue.h"

// sbn includes
/*#include "sbnanaobj/StandardRecord/StandardRecord.h"
#include "sbnanaobj/StandardRecord/SRSlice.h"
#include "sbnanaobj/StandardRecord/SRSliceRecoBranch.h"
#include "sbnanaobj/StandardRecord/SRPFP.h"
*/
// std incldes
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

void makeHists_db_postgre(std::string inFileName,
                          std::string outFileName,
                          unsigned int minRun = std::numeric_limits<unsigned int>::min(),//max //Era 1 starts run1825
                          unsigned int maxRun = std::numeric_limits<unsigned int>::max(),//min //Era 1 ends circa run18593
                          bool debug = false)
{
  // get the weights stored correctly
  TH1::SetDefaultSumw2(true);

  gInterpreter->GenerateDictionary("vector<vector<float>>", "vector");  
  gInterpreter->GenerateDictionary("vector<vector<int>>", "vector"); 

  // open the file and get the StandardRecord TTree
  //std::unique_ptr<TFile> inFile(TFile::Open(inFileName.c_str(), "READ"));
  TFile* inFile=new TFile(inFileName.c_str(), "READ");
  if (debug) std::cout << "In file: " << inFile->GetName() << std::endl;

  // set up the reader and values
  // by storing vectors in the TTree we can pluck the leaves as individual values
  TTree* inTree= (TTree*) inFile->Get("data_validation_tree");
  //TTreeReader inTree("data_validation_tree", inFile.get());
  unsigned int                     run                    =0; inTree->SetBranchAddress("run",            &             run                  );
  unsigned int                     subrun                 =0; inTree->SetBranchAddress("subrun",            &          subrun               );
  unsigned int                     event                  =0; inTree->SetBranchAddress("event",            &           event                );
  float                            pot                    =0; inTree->SetBranchAddress("POT",            &             pot                  );
  int                              nslc                   =0; inTree->SetBranchAddress("nslc",            &            nslc                 );
  std::vector<ULong64_t>          * npfp                   =0; inTree->SetBranchAddress("slc.npfp",            &        npfp                 );
  std::vector<char>               * clear_cosmic           =0; inTree->SetBranchAddress("slc.clear_cosmic",         &   clear_cosmic         );   
  std::vector<float>              * CRLongestTrackDirY     =0; inTree->SetBranchAddress("slc.CRLongestTrackDirY",   &   CRLongestTrackDirY   );   
  std::vector<bool>               * FMatchPresent          =0; inTree->SetBranchAddress("slc.FMatchPresent",        &   FMatchPresent        );   
  std::vector<float>              * FMatchLightPE          =0; inTree->SetBranchAddress("slc.FMatchLightPE",        &   FMatchLightPE        );   
  std::vector<float>              * FMatchScore            =0; inTree->SetBranchAddress("slc.FMatchScore",          &   FMatchScore          );   
  std::vector<std::vector<float>> *  trackLength            =0; inTree->SetBranchAddress("slc.pfp.trackLength",      &trackLength          );
  std::vector<std::vector<double>>*  showerLength          =0; inTree->SetBranchAddress("slc.pfp.showerLength",     &   showerLength         );
  std::vector<std::vector<double>>*  trackBestPlane      =0; inTree->SetBranchAddress("slc.pfp.trackBestPlane",   &     trackBestPlane       );
  std::vector<std::vector<double>>*  showerBestPlane     =0; inTree->SetBranchAddress("slc.pfp.showerBestPlane",  &     showerBestPlane      );
  std::vector<std::vector<double>>*  trackDirY             =0; inTree->SetBranchAddress("slc.pfp.trackDirY",  &         trackDirY            );
  std::vector<std::vector<double>>*  trackVtxX             =0; inTree->SetBranchAddress("slc.pfp.trackVtxX",  &         trackVtxX            );
  std::vector<std::vector<double>>*  trackVtxY             =0; inTree->SetBranchAddress("slc.pfp.trackVtxY",  &         trackVtxY            );
  std::vector<std::vector<double>>*  trackVtxZ             =0; inTree->SetBranchAddress("slc.pfp.trackVtxZ",  &         trackVtxZ            );
  std::vector<std::vector<double>>*  trackNHit1          =0; inTree->SetBranchAddress("slc.pfp.trackNHit1",  &          trackNHit1           );
  std::vector<std::vector<double>>*  trackNHit2          =0; inTree->SetBranchAddress("slc.pfp.trackNHit2",  &          trackNHit2           );
  std::vector<std::vector<double>>*  trackNHit3          =0; inTree->SetBranchAddress("slc.pfp.trackNHit3",  &          trackNHit3           );
  int                               nflash                 =0; inTree->SetBranchAddress("nflash",            &        nflash                 );
  std::vector<float>              * flashTimeWidth         =0; inTree->SetBranchAddress("flash.timeWidth",     &      flashTimeWidth         );
  std::vector<float>              * flashTimeSD            =0; inTree->SetBranchAddress("flash.timeSD",        &      flashTimeSD            );
  std::vector<float>              * flashPE                =0; inTree->SetBranchAddress("flash.PE",            &      flashPE                );
  int                              nCRTHit                =0; inTree->SetBranchAddress("ncrthit",            &       nCRTHit                );
  std::vector<int>                * CRTHitPlane            =0; inTree->SetBranchAddress("crt_hit.plane",       &      CRTHitPlane            );
  std::vector<float>              * CRTHitPE               =0; inTree->SetBranchAddress("crt_hit.PE",           &     CRTHitPE               );
  std::vector<float>              * CRTHitErrX             =0; inTree->SetBranchAddress("crt_hit.err_x",       &      CRTHitErrX             );
  std::vector<float>              * CRTHitErrY             =0; inTree->SetBranchAddress("crt_hit.err_y",       &      CRTHitErrY             );
  std::vector<float>              * CRTHitErrZ             =0; inTree->SetBranchAddress("crt_hit.err_z",       &      CRTHitErrZ             );
  int                              nCRTTrack              =0; inTree->SetBranchAddress("ncrt_track",          &      nCRTTrack              );
  std::vector<float>              * CRTTrackTime           =0; inTree->SetBranchAddress("crt_track.time",      &      CRTTrackTime           );
  int                              nCRTPMTMatch           =0; inTree->SetBranchAddress("ncrtpmt_match",       &      nCRTPMTMatch           );   
  std::vector<int>                * nCRTPMTMatchHit        =0; inTree->SetBranchAddress("crtpmt_match.nhit",   &      nCRTPMTMatchHit        );   
  std::vector<std::vector<double>>* nCRTPMTMatchHitTimeDiff=0; inTree->SetBranchAddress("crtpmt_match.hit.timeDiff", &nCRTPMTMatchHitTimeDiff);         
  /*bool                             dbRunInfoExists        =0; inTree->SetBranchAddress("db.runinfo_exists",    &     dbRunInfoExists        );    
  bool                             dbTriggerDataExists    =0; inTree->SetBranchAddress("db.triggerdata_exists", &    dbTriggerDataExists    );    
  int                              dbRun                  =0; inTree->SetBranchAddress("db.run",     &               dbRun                  );
  int                              dbStart                =0; inTree->SetBranchAddress("db.start",     &             dbStart                );
  int                              dbEnd                  =0; inTree->SetBranchAddress("db.end",     &               dbEnd                  );
  TString                          dbConfig               =""; inTree->SetBranchAddress("db.config",     &            dbConfig               );
  double                           dbCath                 =0; inTree->SetBranchAddress("db.cathodeV",     &          dbCath                 );
  double                           dbEInd1                =0; inTree->SetBranchAddress("db.EInd1V",     &            dbEInd1                );
  double                           dbEInd2                =0; inTree->SetBranchAddress("db.EInd2V",     &            dbEInd2                );
  double                           dbEColl                =0; inTree->SetBranchAddress("db.ECollV",     &            dbEColl                );
  double                           dbWInd1                =0; inTree->SetBranchAddress("db.WInd1V",     &            dbWInd1                );
  double                           dbWInd2                =0; inTree->SetBranchAddress("db.WInd2V",     &            dbWInd2                );
  double                           dbWColl                =0; inTree->SetBranchAddress("db.WCollV",     &            dbWColl                );
  int                              dbNTPC                 =0; inTree->SetBranchAddress("db.NTPC",     &              dbNTPC                 );
  int                              dbNPMT                 =0; inTree->SetBranchAddress("db.NPMT",     &              dbNPMT                 );
  int                              dbNCRT                 =0; inTree->SetBranchAddress("db.NCRT",     &              dbNCRT                 );
  int                              dbTrigSec              =0; inTree->SetBranchAddress("db.trigSec",    &            dbTrigSec              );
  int                              dbTrigNanosec          =0; inTree->SetBranchAddress("db.trigNanosec",    &        dbTrigNanosec          ); 
  int                              dbGateType             =0; inTree->SetBranchAddress("db.gateType",    &           dbGateType             );
  int                              dbTrigSource           =0; inTree->SetBranchAddress("db.trigSource",    &         dbTrigSource           );
  */                                                                                                                                        
  int nEntries=inTree->GetEntries();
  
  // Get Min/Max runs
  /*while (inTree.Next())
  {
    minRun = (minRun < *run) ? minRun : *run;
    maxRun = (maxRun > *run) ? maxRun : *run;
  }
  inTree.Restart();*/
  if (debug) std::cout << "Checking Run " << minRun << " to " << maxRun << std::endl;

  // Open an output file
  std::unique_ptr<TFile> outFile = std::make_unique<TFile>(outFileName.c_str(), "RECREATE");
  outFile->cd();

  // histograms
  unsigned int bins = maxRun - minRun + 1;
  double lwEdge = minRun - 0.5;
  double upEdge = maxRun + 0.5;
  // basic
  std::unique_ptr<TH1D> nEventsPerRun        = std::make_unique<TH1D>("nEventsPerRun",
                                                                      ";DAQ Run;Number of Events",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << nEventsPerRun       ->GetName() << std::endl;
  std::unique_ptr<TH1D> POTPerRun            = std::make_unique<TH1D>("POTPerRun",
                                                                      ";DAQ Run;Protons On Target",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << POTPerRun           ->GetName() << std::endl;
  // DB (fill for each event, but should be normalized for evaluation)
  std::unique_ptr<TH1D> runDuration          = std::make_unique<TH1D>("runDuration",
                                                                      ";DAQ Run;Duration (s)",
                                                                      bins, lwEdge, upEdge);
  std::unique_ptr<TH1D> not_inDB             = std::make_unique<TH1D>("not_inDB",
                                                                      ";DAQ Run;In Database?",
                                                                      bins, lwEdge, upEdge);
  std::unique_ptr<TH1D> not_physics          = std::make_unique<TH1D>("not_physics",
                                                                      ";DAQ Run;Is Not Physics?",
                                                                      bins, lwEdge, upEdge);
  std::unique_ptr<TH1D> test_config          = std::make_unique<TH1D>("test_config",
                                                                      ";DAQ Run;Used Test Config?",
                                                                      bins, lwEdge, upEdge);
  std::unique_ptr<TH1D> cathodeV             = std::make_unique<TH1D>("cathodeV",
                                                                      ";DAQ Run;Cathode Voltage (V)",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << cathodeV            ->GetName() << std::endl;
  std::unique_ptr<TH1D> EInd1V               = std::make_unique<TH1D>("EInd1V",
                                                                      ";DAQ Run;East Induction 1 Voltage (V)",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << EInd1V              ->GetName() << std::endl;
  std::unique_ptr<TH1D> EInd2V               = std::make_unique<TH1D>("EInd2V",
                                                                      ";DAQ Run;East Induction 2 Voltage (V)",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << EInd2V              ->GetName() << std::endl;
  std::unique_ptr<TH1D> ECollV               = std::make_unique<TH1D>("ECollV",
                                                                      ";DAQ Run;East Collection Voltage (V)",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << ECollV              ->GetName() << std::endl;
  std::unique_ptr<TH1D> WInd1V               = std::make_unique<TH1D>("WInd1V",
                                                                      ";DAQ Run;West Induction 1 Voltage (V)",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << WInd1V              ->GetName() << std::endl;
  std::unique_ptr<TH1D> WInd2V               = std::make_unique<TH1D>("WInd2V",
                                                                      ";DAQ Run;West Induction 2 Voltage (V)",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << WInd2V              ->GetName() << std::endl;
  std::unique_ptr<TH1D> WCollV               = std::make_unique<TH1D>("WCollV",
                                                                      ";DAQ Run;West Collection Voltage (V)",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << WCollV              ->GetName() << std::endl;
  std::unique_ptr<TH1D> ETriggers            = std::make_unique<TH1D>("ETriggers",
                                                                      ";DAQ Run;East Cryostat Triggers",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << ETriggers           ->GetName() << std::endl;
  std::unique_ptr<TH1D> WTriggers            = std::make_unique<TH1D>("WTriggers",
                                                                      ";DAQ Run;West Cryostat Triggers",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << WTriggers           ->GetName() << std::endl;
  std::unique_ptr<TH1D> UTriggers            = std::make_unique<TH1D>("UTriggers",
                                                                      ";DAQ Run;Undecided Triggers",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << UTriggers           ->GetName() << std::endl;
  std::unique_ptr<TH1D> BTriggers            = std::make_unique<TH1D>("BTriggers",
                                                                      ";DAQ Run;Triggers From Both Cryostats",
                                                                      bins, lwEdge, upEdge);
  if (debug) std::cout << "initialized histogram " << BTriggers           ->GetName() << std::endl;
  // TPC
  std::unique_ptr<TH2D> nSlicePerEvent       = std::make_unique<TH2D>("nSlicePerEvent",
                                                                      ";DAQ Run;Number of Slices Per Event",
                                                                      bins, lwEdge, upEdge,
                                                                      101,   -0.5, 100.5);
  if (debug) std::cout << "initialized histogram " << nSlicePerEvent      ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nSlicePerEvent_wgt   = std::make_unique<TH2D>("nSlicePerEvent_wgt",
  //                                                                    ";DAQ Run;Slices Per Event (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    101,   -0.5, 100.5);
  //if (debug) std::cout << "initialized histogram " << nSlicePerEvent_wgt  ->GetName() << std::endl;
  std::unique_ptr<TH2D> nPFPPerEvent         = std::make_unique<TH2D>("nPFPPerEvent",
                                                                      ";DAQ Run;Number of PFPs Per Event",
                                                                      bins, lwEdge, upEdge,
                                                                      101,   -0.5, 100.5);
  if (debug) std::cout << "initialized histogram " << nPFPPerEvent        ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nPFPPerEvent_wgt     = std::make_unique<TH2D>("nPFPPerEvent_wgt",
  //                                                                    ";DAQ Run;PFPs Per Event (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    101,   -0.5, 100.5);
  //if (debug) std::cout << "initialized histogram " << nPFPPerEvent_wgt    ->GetName() << std::endl;
  std::unique_ptr<TH2D> nCCPerEvent          = std::make_unique<TH2D>("nCCPerEvent",
                                                                      ";DAQ Run;Number of Clear Cosmics Per Event",
                                                                      bins, lwEdge, upEdge,
                                                                      1001,   -0.5, 1000.5);
  if (debug) std::cout << "initialized histogram " << nCCPerEvent         ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nCCPerEvent_wgt      = std::make_unique<TH2D>("nCCPerEvent_wgt",
  //                                                                    ";DAQ Run;Clear Cosmics Per Event (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    1001,   -0.5, 1000.5);
  //if (debug) std::cout << "initialized histogram " << nCCPerEvent_wgt     ->GetName() << std::endl;
  std::unique_ptr<TH2D> nNeutrinoPure        = std::make_unique<TH2D>("nNeutrinoPure",
                                                                      ";DAQ Run;Beam-like Slices per Event",
                                                                      bins, lwEdge, upEdge,
                                                                      26,   -0.5, 25.5);
  if (debug) std::cout << "initialized histogram " << nNeutrinoPure       ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nNeutrinoPure_wgt    = std::make_unique<TH2D>("nNeutrinoPure_wgt",
  //                                                                    ";DAQ Run;Beam-like Slices per Event (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    26,   -0.5, 25.5);
  //if (debug) std::cout << "initialized histogram " << nNeutrinoPure_wgt   ->GetName() << std::endl;
  std::unique_ptr<TH2D> nTrkHitsPlane1       = std::make_unique<TH2D>("nTrkHitsPlane1",
                                                                      ";DAQ Run;Number of Hits in Ind1",
                                                                      bins, lwEdge, upEdge,
                                                                      40001,   -0.5, 40000.5);
  if (debug) std::cout << "initialized histogram " << nTrkHitsPlane1      ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nTrkHitsPlane1_wgt   = std::make_unique<TH2D>("nTrkHitsPlane1_wgt",
  //                                                                    ";DAQ Run;Number of Hits in Ind1 (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    40001,   -0.5, 40000.5);
  //if (debug) std::cout << "initialized histogram " << nTrkHitsPlane1_wgt  ->GetName() << std::endl;
  std::unique_ptr<TH2D> nTrkHitsPlane2       = std::make_unique<TH2D>("nTrkHitsPlane2",
                                                                      ";DAQ Run;Number of Hits in Ind2",
                                                                      bins, lwEdge, upEdge,
                                                                      40001,   -0.5, 40000.5);
  if (debug) std::cout << "initialized histogram " << nTrkHitsPlane2      ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nTrkHitsPlane2_wgt   = std::make_unique<TH2D>("nTrkHitsPlane2_wgt",
  //                                                                    ";DAQ Run;Number of Hits in Ind2 (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    40001,   -0.5, 40000.5);
  //if (debug) std::cout << "initialized histogram " << nTrkHitsPlane2_wgt  ->GetName() << std::endl;
  std::unique_ptr<TH2D> nTrkHitsPlane3       = std::make_unique<TH2D>("nTrkHitsPlane3",
                                                                      ";DAQ Run;Number of Hits in Coll",
                                                                      bins, lwEdge, upEdge,
                                                                      40001,   -0.5, 40000.5);
  if (debug) std::cout << "initialized histogram " << nTrkHitsPlane3      ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nTrkHitsPlane3_wgt   = std::make_unique<TH2D>("nTrkHitsPlane3_wgt",
  //                                                                    ";DAQ Run;Number of Hits in Coll (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    40001,   -0.5, 40000.5);
  //if (debug) std::cout << "initialized histogram " << nTrkHitsPlane3_wgt  ->GetName() << std::endl;
  std::unique_ptr<TH2D> nHitPerTrkPlane1     = std::make_unique<TH2D>("nHitPerTrkPlane1",
                                                                      ";DAQ Run;Number of Ind1 Hits per Track",
                                                                      bins, lwEdge, upEdge,
                                                                      5001,   -0.5, 100.5);
  if (debug) std::cout << "initialized histogram " << nHitPerTrkPlane1    ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nHitPerTrkPlane1_wgt = std::make_unique<TH2D>("nHitPerTrkPlane1_wgt",
  //                                                                    ";DAQ Run;Number of Ind1 Hits per Track (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    5001,   -0.5, 100.5);
  //if (debug) std::cout << "initialized histogram " << nHitPerTrkPlane1_wgt->GetName() << std::endl;
  std::unique_ptr<TH2D> nHitPerTrkPlane2     = std::make_unique<TH2D>("nHitPerTrkPlane2",
                                                                      ";DAQ Run;Number of Ind2 Hits per Track",
                                                                      bins, lwEdge, upEdge,
                                                                      5001,   -0.5, 100.5);
  if (debug) std::cout << "initialized histogram " << nHitPerTrkPlane2    ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nHitPerTrkPlane2_wgt = std::make_unique<TH2D>("nHitPerTrkPlane2_wgt",
  //                                                                    ";DAQ Run;Number of Ind2 Hits per Track (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    5001,   -0.5, 100.5);
  //if (debug) std::cout << "initialized histogram " << nHitPerTrkPlane2_wgt->GetName() << std::endl;
  std::unique_ptr<TH2D> nHitPerTrkPlane3     = std::make_unique<TH2D>("nHitPerTrkPlane3",
                                                                      ";DAQ Run;Number of Coll Hits per Track",
                                                                      bins, lwEdge, upEdge,
                                                                      5001,   -0.5, 100.5);
  if (debug) std::cout << "initialized histogram " << nHitPerTrkPlane3    ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nHitPerTrkPlane3_wgt = std::make_unique<TH2D>("nHitPerTrkPlane3_wgt",
  //                                                                    ";DAQ Run;Number of Coll Hits per Track (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    5001,   -0.5, 100.5);
  //if (debug) std::cout << "initialized histogram " << nHitPerTrkPlane3_wgt->GetName() << std::endl;

  // PMT
  std::unique_ptr<TH2D> nFlashPerEvent       = std::make_unique<TH2D>("nFlashPerEvent",
                                                                      ";DAQ Run;Number of PMT FLashes",
                                                                      bins, lwEdge, upEdge,
                                                                      101,   -0.5, 100.5);
  if (debug) std::cout << "initialized histogram " << nFlashPerEvent      ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nFlashPerEvent_wgt   = std::make_unique<TH2D>("nFlashPerEvent_wgt",
  //                                                                    ";DAQ Run;Number of PMT FLashes (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    101,   -0.5, 100.5);
  //if (debug) std::cout << "initialized histogram " << nFlashPerEvent_wgt  ->GetName() << std::endl;
  std::unique_ptr<TH2D> flashPerCC           = std::make_unique<TH2D>("flashPerCC",
                                                                      ";DAQ Run;Number of PMT FLashes per Clear Cosmic",
                                                                      bins, lwEdge, upEdge,
                                                                      1001, -0.5, 1.5);
  if (debug) std::cout << "initialized histogram " << flashPerCC          ->GetName() << std::endl;
  //std::unique_ptr<TH2D> flashPerCC_wgt       = std::make_unique<TH2D>("flashPerCC_wgt",
  //                                                                    ";DAQ Run;Number of PMT FLashes per Clear Cosmic (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    1001, -0.5, 1.5);
  //if (debug) std::cout << "initialized histogram " << flashPerCC_wgt      ->GetName() << std::endl;

  std::unique_ptr<TH2D> FlashTimeWidth       = std::make_unique<TH2D>("FlashTimeWidth",
                                                                      ";DAQ Run;Average Flash Width",
                                                                      bins, lwEdge, upEdge,
                                                                      101,   -0.5, 10.5);
  if (debug) std::cout << "initialized histogram " << FlashTimeWidth      ->GetName() << std::endl;
  //std::unique_ptr<TH2D> FlashTimeWidth_wgt   = std::make_unique<TH2D>("FlashTimeWidth_wgt",
  //                                                                    ";DAQ Run;Average Flash Width (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    101,   -0.5, 10.5);
  //if (debug) std::cout << "initialized histogram " << FlashTimeWidth_wgt  ->GetName() << std::endl;
  std::unique_ptr<TH2D> FlashTimeSD          = std::make_unique<TH2D>("FlashTimeSD",
                                                                      ";DAQ Run;Average Flash SD",
                                                                      bins, lwEdge, upEdge,
                                                                      510,   -0.5, 5.5);
  if (debug) std::cout << "initialized histogram " << FlashTimeSD         ->GetName() << std::endl;
  //std::unique_ptr<TH2D> FlashTimeSD_wgt      = std::make_unique<TH2D>("FlashTimeSD_wgt",
  //                                                                    ";DAQ Run;Average Flash SD (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    510,   -0.5, 5.5);
  //if (debug) std::cout << "initialized histogram " << FlashTimeSD_wgt     ->GetName() << std::endl;
  std::unique_ptr<TH2D> FlashPE              = std::make_unique<TH2D>("FlashPE",
                                                                      ";DAQ Run;Average Flash PE",
                                                                      bins, lwEdge, upEdge,
                                                                      20001,   -0.5, 200000.5);
  if (debug) std::cout << "initialized histogram " << FlashPE             ->GetName() << std::endl;
  //std::unique_ptr<TH2D> FlashPE_wgt          = std::make_unique<TH2D>("FlashPE_wgt",
  //                                                                    ";DAQ Run;Average Flash PE (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    20001,   -0.5, 200000.5);
  //if (debug) std::cout << "initialized histogram " << FlashPE_wgt         ->GetName() << std::endl;
  // CRT
  //std::unique_ptr<TH2D> NCRTHit              = std::make_unique<TH2D>("NCRTHit",
  //                                                                    ";DAQ Run;CRT Hits",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    501,   -0.5, 500.5);
  //std::unique_ptr<TH2D> NCRTHit_wgt          = std::make_unique<TH2D>("NCRTHit_wgt",
  //                                                                    ";DAQ Run;CRT Hits (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    501,   -0.5, 500.5);
  //std::unique_ptr<TH2D> CRTHitPEPerHit       = std::make_unique<TH2D>("CRTHitPEPerHit",
  //                                                                    ";DAQ Run;CRT PE Per Event",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    50001,   -0.5, 5000.5);
  //std::unique_ptr<TH2D> CRTHitPEPerHit_wgt   = std::make_unique<TH2D>("CRTHitPEPerHit_wgt",
  //                                                                    ";DAQ Run;CRT PE Per Hit (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    50001,   -0.5, 5000.5);
  //std::unique_ptr<TH2D> avgCRTHitErr         = std::make_unique<TH2D>("avgCRTHitErr",
  //                                                                    ";DAQ Run;Average CRT Hit Error",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    5001,   -0.5, 500.5);
  //std::unique_ptr<TH2D> avgCRTHitErr_wgt     = std::make_unique<TH2D>("avgCRTHitErr_wgt",
  //                                                                    ";DAQ Run;Average CRT Hit Error (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    5001,   -0.5, 500.5);
  //std::unique_ptr<TH2D> nCRTTrkPerEvent      = std::make_unique<TH2D>("nCRTTrkPerEvent",
  //                                                                    ";DAQ Run;Average CRT Tracks Per Event",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    251,   -0.5, 250.5);
  //std::unique_ptr<TH2D> nCRTTrkPerEvent_wgt  = std::make_unique<TH2D>("nCRTTrkPerEvent_wgt",
  //                                                                    ";DAQ Run;Average CRT Tracks Per Event (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    251,   -0.5, 250.5);
  //std::unique_ptr<TH2D> CRTTrkTime           = std::make_unique<TH2D>("CRTTrkTime",
  //                                                                    ";DAQ Run;Average CRT Track Time",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    101,   -0.5, 10.5);
  //std::unique_ptr<TH2D> CRTTrkTime_wgt       = std::make_unique<TH2D>("CRTTrkTime_wgt",
  //                                                                    ";DAQ Run;Average CRT Track Time (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    101,   -0.5, 10.5);
  std::unique_ptr<TH2D> nMatchesPerEvent     = std::make_unique<TH2D>("nMatchesPerEvent",
                                                                      ";DAQ Run;Flash Matches Per Event",
                                                                      bins, lwEdge, upEdge,
                                                                      51,   -0.5, 50.5);
  if (debug) std::cout << "initialized histogram " << nMatchesPerEvent    ->GetName() << std::endl;
  //std::unique_ptr<TH2D> nMatchesPerEvent_wgt = std::make_unique<TH2D>("nMatchesPerEvent_wgt",
  //                                                                    ";DAQ Run;Flash Matches Per Event (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    51,   -0.5, 50.5);
  //if (debug) std::cout << "initialized histogram " << nMatchesPerEvent_wgt->GetName() << std::endl;
  //std::unique_ptr<TH2D> nMchHitsPerEvent     = std::make_unique<TH2D>("nMchHitsPerEvent",
  //                                                                    ";DAQ Run;Flash Match Hits Per Event",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    101,   -0.5, 100.5);
  //std::unique_ptr<TH2D> nMchHitsPerEvent_wgt = std::make_unique<TH2D>("nMchHitsPerEvent_wgt",
  //                                                                    ";DAQ Run;Flash Match Hits Per Event (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    101,   -0.5, 100.5);
  //std::unique_ptr<TH2D> fmTimeDiff           = std::make_unique<TH2D>("fmTimeDiff",
  //                                                                    ";DAQ Run;Flash Match Time Difference",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    1001,   -0.5, 1.5);
  //std::unique_ptr<TH2D> fmTimeDiff_wgt       = std::make_unique<TH2D>("fmTimeDiff_wgt",
  //                                                                    ";DAQ Run;Flash Match Time Difference (POT Weighted)",
  //                                                                    bins, lwEdge, upEdge,
  //                                                                    1001,   -0.5, 1.5);
// read the TTree
  size_t nEvt = 1;
  if (debug) std::cout << "READY TO LOOP" << std::endl;
  int loopcount=0;
  if (debug) cout<< "Will loop over "<<nEntries<<" entries"<<endl;
  //while (inTree.Next())
  for(int iEntry=0; iEntry<nEntries; iEntry++){
    cout<<"iEntry: "<<iEntry<<endl;
    inTree->GetEntry(iEntry);
    cout<<"Run: "<<run<<endl;
    if(debug) cout<<"Loop count: "<<loopcount<<endl;
    loopcount++;
    // skip explicit tests and non-physics on beam streams
    //if ((not dbRunInfoExists) || (not dbTriggerDataExists))
    /*if (not dbTriggerDataExists)
      not_inDB->Fill(run);
    if ((not (dbConfig.Contains("physics", TString::kIgnoreCase))) || ((dbGateType == 1) || (dbGateType == 2)))
      not_physics->Fill(run);
    if (dbConfig.Contains("test", TString::kIgnoreCase))
      test_config->Fill(run);
    */
    // fill the ones which require no processing
    nEventsPerRun->Fill(run);
    POTPerRun->Fill(run, pot);
    nSlicePerEvent->Fill(run, nslc);
  
    /*
    runDuration->Fill(run, dbEnd - dbStart);
    cathodeV ->Fill(run, dbCath );
    EInd1V   ->Fill(run, dbEInd1);
    EInd2V   ->Fill(run, dbEInd2);
    ECollV   ->Fill(run, dbEColl);
    WInd1V   ->Fill(run, dbWInd1);
    WInd2V   ->Fill(run, dbWInd2);
    WCollV   ->Fill(run, dbWColl);
    switch (dbTrigSource)
    {
      case 1:
        ETriggers->Fill(run);
        break;
      case 2:
        WTriggers->Fill(run);
        break;
      case 0:
        UTriggers->Fill(run);
        break;
      case 7:
        BTriggers->Fill(run);
        break;
      default:
        std::cerr << "CANNOT RESOLVE TRIGGER SOURCE " << dbTrigSource << std::endl;
        break;
    }
    */
    //nSlicePerEvent_wgt->Fill(run, nslc, pot);
    nFlashPerEvent->Fill(run, nflash);
    //nFlashPerEvent_wgt->Fill(run, nflash, pot);
    //NCRTHit->Fill(run, nCRTHit);
    //NCRTHit_wgt->Fill(run, nCRTHit, pot);
    //nCRTTrkPerEvent->Fill(run, nCRTTrack);
    //nCRTTrkPerEvent_wgt->Fill(run, nCRTTrack, pot);
    //nMatchesPerEvent->Fill(run, nCRTPMTMatch);
    //nMatchesPerEvent_wgt->Fill(run, nCRTPMTMatch, pot);

    // initialize counters/variables
    size_t nClearCosmics = 0;
    size_t nNeutrinoCandidate = 0;
    size_t nTrackHits1 = 0;
    size_t nTrackHits2 = 0;
    size_t nTrackHits3 = 0;
    size_t nTracks = 0;
    float  crtHitErr = std::numeric_limits<float>::lowest();

    // loop over the slices
    for (size_t slc_idx = 0; slc_idx < nslc; ++slc_idx)
    {
      // Fill simple slice hists
      nPFPPerEvent->Fill(run, npfp->at(slc_idx));
      //nPFPPerEvent_wgt->Fill(run, npfp->at(slc_idx), pot);

      // loop over the PFPs
      for (size_t pfp_idx = 0; pfp_idx < npfp->at(slc_idx); ++pfp_idx)
      {
        //cout<<"slc: "<<slc_idx<<" pfp: "<<pfp_idx<<endl;
        //cout<<trackLength->size()<<endl;
        // check whether it's better as a track or a shower
        bool trkGood = (trackLength->at(slc_idx)[pfp_idx] > 0);
        bool shwGood = (showerLength->at(slc_idx)[pfp_idx] > 0);
        if (not trkGood && not shwGood)
          continue;

        // use the shower info only if the track info isn't good
        float pfpLength = (trkGood) ? trackLength->at(slc_idx)[pfp_idx]
                                    : showerLength->at(slc_idx)[pfp_idx];
        int   pfpPlane  = (trkGood) ? trackBestPlane->at(slc_idx)[pfp_idx]
                                    : showerBestPlane->at(slc_idx)[pfp_idx];

        // get PFP level info
        if (trkGood)
        {
          nTrackHits1 += trackNHit1->at(slc_idx)[pfp_idx];
          nTrackHits2 += trackNHit2->at(slc_idx)[pfp_idx];
          nTrackHits3 += trackNHit3->at(slc_idx)[pfp_idx];
          ++nTracks;
        }
        if (clear_cosmic->at(slc_idx))
        {
          ++nClearCosmics;
        } else if (std::abs(CRLongestTrackDirY->at(slc_idx)) < 0.1) {
          ++nNeutrinoCandidate;
        }
      }
    }

    // loop over flashes
    for (size_t flsh_idx = 0; flsh_idx < nflash; ++flsh_idx)
    {
      FlashTimeWidth->Fill(run, flashTimeWidth->at(flsh_idx));
      //FlashTimeWidth_wgt->Fill(run, flashTimeWidth->at(flsh_idx), pot);
      FlashTimeSD->Fill(run, flashTimeSD->at(flsh_idx));
      //FlashTimeSD_wgt->Fill(run, flashTimeSD->at(flsh_idx), pot);
      FlashPE->Fill(run, flashPE->at(flsh_idx));
      //FlashPE_wgt->Fill(run, flashPE->at(flsh_idx), pot);
    }

    // loop over CRT hits
    //for (size_t crt_hit_idx = 0; crt_hit_idx < nCRTHit; ++crt_hit_idx)
    //{
    //  CRTHitPEPerHit->Fill(run, CRTHitPE->at(crt_hit_idx));
    //  CRTHitPEPerHit_wgt->Fill(run, CRTHitPE->at(crt_hit_idx), pot);
    //  crtHitErr = std::sqrt(std::pow(CRTHitErrX->at(crt_hit_idx), 2)
    //                      + std::pow(CRTHitErrY->at(crt_hit_idx), 2)
    //                      + std::pow(CRTHitErrZ->at(crt_hit_idx), 2));
    //  avgCRTHitErr->Fill(run, crtHitErr);
    //  avgCRTHitErr_wgt->Fill(run, crtHitErr, pot);
    //}

    // loop over CRT tracks
    //for (size_t crt_trk_idx = 0; crt_trk_idx < nCRTTrack; ++crt_trk_idx)
    //{
    //  CRTTrkTime->Fill(run, CRTTrackTime->at(crt_trk_idx));
    //  CRTTrkTime_wgt->Fill(run, CRTTrackTime->at(crt_trk_idx), pot);
    //}

    // loop over CRT PMT matches
    //for (size_t mch_idx = 0; mch_idx < nCRTPMTMatch; ++mch_idx)
    //{
    //  nMchHitsPerEvent->Fill(run, nCRTPMTMatchHit->at(mch_idx));
    //  nMchHitsPerEvent_wgt->Fill(run, nCRTPMTMatchHit->at(mch_idx));
    //  // loop over the match hits
    //  for (size_t hit_idx = 0; hit_idx < nCRTPMTMatchHit->at(mch_idx); ++hit_idx)
    //  {
    //    fmTimeDiff->Fill(run, nCRTPMTMatchHitTimeDiff->at(mch_idx)[hit_idx]);
    //    fmTimeDiff_wgt->Fill(run, nCRTPMTMatchHitTimeDiff->at(mch_idx)[hit_idx], pot);
    //  }
    //}

    // fill the more complex ones
    nCCPerEvent->Fill(run, nClearCosmics);
    //nCCPerEvent_wgt->Fill(run, nClearCosmics, pot);
    flashPerCC->Fill(run, static_cast<double>(nflash) / static_cast<double>(nClearCosmics));
    //flashPerCC_wgt->Fill(run, static_cast<double>(nflash) / static_cast<double>(nClearCosmics), pot);
    nNeutrinoPure->Fill(run, nNeutrinoCandidate);
    //nNeutrinoPure_wgt->Fill(run, nNeutrinoCandidate, pot);
    nTrkHitsPlane1->Fill(run, nTrackHits1);
    //nTrkHitsPlane1_wgt->Fill(run, nTrackHits1, pot);
    nTrkHitsPlane2->Fill(run, nTrackHits2);
    //nTrkHitsPlane2_wgt->Fill(run, nTrackHits2, pot);
    nTrkHitsPlane3->Fill(run, nTrackHits3);
    //nTrkHitsPlane3_wgt->Fill(run, nTrackHits3, pot);
    nHitPerTrkPlane1->Fill(run, static_cast<double>(nTrackHits1) / static_cast<double>(nTracks));
    //nHitPerTrkPlane1_wgt->Fill(run, static_cast<double>(nTrackHits1) / static_cast<double>(nTracks), pot);
    nHitPerTrkPlane2->Fill(run, static_cast<double>(nTrackHits2) / static_cast<double>(nTracks));
    //nHitPerTrkPlane2_wgt->Fill(run, static_cast<double>(nTrackHits2) / static_cast<double>(nTracks), pot);
    nHitPerTrkPlane3->Fill(run, static_cast<double>(nTrackHits3) / static_cast<double>(nTracks));
    //nHitPerTrkPlane3_wgt->Fill(run, static_cast<double>(nTrackHits3) / static_cast<double>(nTracks), pot);
  }

  outFile->Write();
  if (debug) std::cout << "Out file " << outFile->GetName() << " contains" << std::endl;
  if (debug) outFile->ls();
  outFile->Close();

  // clean up unique_ptrs
  nEventsPerRun .release();
  POTPerRun     .release();
  runDuration   .release();
  not_inDB      .release();
  not_physics   .release();
  test_config   .release();
  cathodeV      .release();
  EInd1V        .release();
  EInd2V        .release();
  ECollV        .release();
  WInd1V        .release();
  WInd2V        .release();
  WCollV        .release();
  ETriggers     .release();
  WTriggers     .release();
  UTriggers     .release();
  BTriggers     .release();
  nSlicePerEvent.release();
  //nSlicePerEvent_wgt.release();
  nFlashPerEvent.release();
  //nFlashPerEvent_wgt.release();
  //NCRTHit.release();
  //NCRTHit_wgt.release();
  //nCRTTrkPerEvent.release();
  //nCRTTrkPerEvent_wgt.release();
  nMatchesPerEvent.release();
  //nMatchesPerEvent_wgt.release();
  nPFPPerEvent.release();
  //nPFPPerEvent_wgt.release();
  FlashTimeWidth.release();
  //FlashTimeWidth_wgt.release();
  FlashTimeSD.release();
  //FlashTimeSD_wgt.release();
  FlashPE.release();
  //FlashPE_wgt.release();
  //CRTHitPEPerHit.release();
  //CRTHitPEPerHit_wgt.release();
  //avgCRTHitErr.release();
  //avgCRTHitErr_wgt.release();
  //CRTTrkTime.release();
  //CRTTrkTime_wgt.release();
  //nMchHitsPerEvent.release();
  //nMchHitsPerEvent_wgt.release();
  //fmTimeDiff.release();
  //fmTimeDiff_wgt.release();
  nCCPerEvent.release();
  //nCCPerEvent_wgt.release();
  flashPerCC.release();
  //flashPerCC_wgt.release();
  nNeutrinoPure.release();
  //nNeutrinoPure_wgt.release();
  nTrkHitsPlane1.release();
  //nTrkHitsPlane1_wgt.release();
  nTrkHitsPlane2.release();
  //nTrkHitsPlane2_wgt.release();
  nTrkHitsPlane3.release();
  //nTrkHitsPlane3_wgt.release();
  nHitPerTrkPlane1.release();
  //nHitPerTrkPlane1_wgt.release();
  nHitPerTrkPlane2.release();
  //nHitPerTrkPlane2_wgt.release();
  nHitPerTrkPlane3.release();
  //nHitPerTrkPlane3_wgt.release();
}
