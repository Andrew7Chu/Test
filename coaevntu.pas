{ Copyright (C) 2000-2007 Kisters Pty Ltd. }
unit coaevntu;

interface

uses
  Winapi.Windows,  Winapi.Messages, System.SysUtils, System.Classes,  Vcl.Graphics,
  Vcl.Controls,    Vcl.Forms,       Vcl.Dialogs,     System.Math,     Data.Win.ADODB,
  HyDef,           HyDSSpec,        HyFileT,         HySList,         HySListX,
  hylist;

{.
.Function
.Class
.Owner    DJE
.}

type
  TProgMode   = (pmCompute,pmUpdate,pmValidate,pmStatistics,pmEMC);
  TRainMode   = (rmNone,rmRain,rmFlow,rmBoth,rmExtra);
  TFlowMode   = (fmNone,fmLevel,fmFlow,fmBFS);
  TWriteMode  = (wmInsert,wmUpdate,wmBoth,wmAdd);
  TInsertType = (itPointValue,itZeroValue,itCommentOnly);
  TSampleType = (stEMC,stStormWater,stExcluded,stBaseFlow,stPrimary,stSecondary);

  TDataPoint = record
    Time: TRelTime;
    Value: TReal;
    Quality: Integer;
  end;

  TMySortedValueList = class(TSortedValueList)
  private
    function GetMedian: TReal;
  public
    property Median: TReal read GetMedian;
  end;

  TADODB = class
  private
    fADOConnection: TADOConnection;
    fADOQuery: TADOQuery;
  public
    constructor Create(ConnectStr: String);
    destructor Destroy; override;
    procedure AddSQL(SQLStatement: String);
    property ADOQuery: TADOQuery read fADOQuery;
  end;

  TTableRecord = class
  private
    fFieldNames: TStringList;
    fFieldValues: TStringList;
    fFieldTypes: TStringList;
    fWhere: TStringList;
    fSetList: TStringList;
    function GetFieldNames: String; virtual;
    function GetFieldValues: String; virtual;
    function GetFieldTypes: String; virtual;
    function GetWhere: String; virtual;
    function GetSetList: String; virtual;
    function GetDisplayText: String; virtual;
    function Quote(Str: String): String;
  public
    constructor Create;
    destructor Destroy; override;
    property FieldNames: String read GetFieldNames;
    property FieldValues: String read GetFieldValues;
    property FieldTypes: String read GetFieldTypes;
    property Where: String read GetWhere;
    property SetList: String read GetSetList;
    property DisplayText: String read GetDisplayText;
  end;

  TMyAutoList = class(TAutoList)
  public
    constructor Create;
    procedure WriteToTable(DB: TADODB; TableName: String; WriteMode: TWriteMode);
  end;

  TEventParams = class
  private
    fSite: TStID;
    fStartDate: TRelTime;
    fSitePeriod,fStartExtendPeriod,fEndExtendPeriod: Integer;
    function GetSiteInterval: Integer;
    function GetStartExtendInterval: TReal;
    function GetEndExtendInterval: TReal;
  public
    constructor Create(aSite: TStID; StartDate: TRelTime;
      SitePeriod,StartExtendPeriod,EndExtendPeriod: Integer);
    property epSite: TStID read fSite;
    property StartDate: TRelTime read fStartDate;
    property SiteInterval: Integer read GetSiteInterval;
    property StartExtendInterval: TReal read GetStartExtendInterval;
    property EndExtendInterval: TReal read GetEndExtendInterval;
  end;

{ Each record from the Site_Flow_Event_Parameters table is loaded into a TFlowEventParams object }
  TFlowEventParams = class(TEventParams)
  private
    fMethod: TFlowMode;
    fTimeThreshold,fLevelThreshold,fFlowThreshold,fVolumeThreshold,fRecessionThreshold: TReal;
    fRisePeriod,fDropPeriod,fDelayPeriod: Integer;
    function GetRiseInterval: TReal;
    function GetDropInterval: TReal;
    function GetDelayInterval: TReal;
  public
    constructor Create(
      aSite: TStID; StartDate: TRelTime;
      SitePeriod,StartExtendPeriod,EndExtendPeriod: Integer;
      Method: TFlowMode;
      TimeThreshold,LevelThreshold,FlowThreshold,VolumeThreshold,RecessionThreshold: TReal;
      RisePeriod,DropPeriod,DelayPeriod: Integer);
    property Method: TFlowMode read fMethod;
    property TimeThreshold: TReal read fTimeThreshold;
    property LevelThreshold: TReal read fLevelThreshold;
    property FlowThreshold: TReal read fFlowThreshold;
    property VolumeThreshold: TReal read fVolumeThreshold;
    property RecessionThreshold: TReal read fRecessionThreshold;
    property RiseInterval: TReal read GetRiseInterval;
    property DropInterval: TReal read GetDropInterval;
    property DelayInterval: TReal read GetDelayInterval;
    property DropPeriod: Integer read fDropPeriod;
  end;

{ Each record from the Site_Rain_Event_Parameters table is loaded into a TRainEventParams object }
  TRainEventParams = class(TEventParams)
  private
    fIntervalPeriod: Integer;
    fMinIntervalTotal,fMinEventTotal: TReal;
    function GetRainInterval: TReal;
    function GetDrizzleRate: TReal;
  public
    constructor Create(
      aSite: TStID; StartDate: TRelTime; SitePeriod,ExtendPeriod: Integer;
      IntervalPeriod: Integer;
      MinIntervalTotal,MinEventTotal: TReal);
    property MinIntervalTotal: TReal read fMinIntervalTotal;
    property MinEventTotal: TReal read fMinEventTotal;
    property RainInterval: TReal read GetRainInterval;
    property DrizzleRate: TReal read GetDrizzleRate;
  end;

  TEventParamsList = class(TMyAutoList)
  public
    function Find(aSite: TStID; Time: TRelTime): TEventParams;
  end;

{ Details from the Site_Flow_Event_Parameters table are loaded into this object }
  TFlowEventParamsList = class(TEventParamsList)
  public
    procedure LoadRecords(DB: TADODB; TableName: String);
  end;

{ Details from the Site_Rain_Event_Parameters table are loaded into this object }
  TRainEventParamsList = class(TEventParamsList)
  public
    procedure LoadRecords(DB: TADODB; TableName: String);
  end;

  TEventStatistic = class;
  TEventStatisticList = class;
  TEMC = class;
  TEMCList = class;
  TSiteDetails = class;

{ Each record for the Flow_Event table is loaded into a TEvent object }
  TEvent = class(TTableRecord)
  private
    fSite: TStID;
    fSuffix: string;
    fID: Integer;
    fEventID,fNewEventID: String;
    fRainMode: TRainMode;
    fRainModeNum: Integer;
    fStartOfRain,fEndOfRain: TRelTime;
    fTotalRain,fIntensity: TReal;
    fRainQual: Integer;
    fFlowMode: TFlowMode;
    fStartOfFlow,fEndOfFlow: TRelTime;
    fTotalFlow: TReal;
    fFlowQual: Integer;
    fFlowValid,fEMCValid: Boolean;
    fStartOfStorm,fEndOfStorm: TRelTime;
    fStatistics: TEventStatisticList;
    fEMCList: TEMCList;
    function GetStartOfEvent: TRelTime;
    function GetEndOfEvent: TRelTime;
    function GetRV: TEventStatistic;
    function GetKey: String;
    function GetFieldNames: String; override;
    function GetFieldValues: String; override;
    function GetWhere: String; override;
    function GetSetList: String; override;
    function GetDisplayText: String; override;
  public
    constructor Create(aSite: TStID; Suffix: string; ID: Integer); overload;
    constructor Create(Event: TEvent); overload;
    destructor Destroy; override;
    property RV: TEventStatistic read GetRV;
    property Key: String read GetKey;

    property evSite: TStID read fSite;
    property Suffix: string read fSuffix;
    property StartOfEvent: TRelTime read GetStartOfEvent;
    property EndOfEvent: TRelTime read GetEndOfEvent;
    property ID: Integer read fID write fID;
    property EventID: String read fEventID write fEventID;
    property NewEventID: String read fNewEventID write fNewEventID;
    property RainMode: TRainMode read fRainMode write fRainMode;
    property RainModeNum: Integer read fRainModeNum write fRainModeNum;
    property StartOfRain: TRelTime read fStartOfRain write fStartOfRain;
    property EndOfRain: TRelTime read fEndOfRain write fEndOfRain;
    property TotalRain: TReal read fTotalRain write fTotalRain;
    property Intensity: TReal read fIntensity write fIntensity;
    property RainQual: Integer read fRainQual write fRainQual;
    property FlowMode: TFlowMode read fFlowMode write fFlowMode;
    property StartOfFlow: TRelTime read fStartOfFlow write fStartOfFlow;
    property EndOfFlow: TRelTime read fEndOfFlow write fEndOfFlow;
    property TotalFlow: TReal read fTotalFlow write fTotalFlow;
    property FlowQual: Integer read fFlowQual write fFlowQual;
    property FlowValid: Boolean read fFlowValid write fFlowValid;
    property EMCValid: Boolean read fEMCValid write fEMCValid;
    property StartOfStorm: TRelTime read fStartOfStorm write fStartOfStorm;
    property EndOfStorm: TRelTime read fEndOfStorm write fEndOfStorm;
    property Statistics: TEventStatisticList read fStatistics write fStatistics;
    property EMCList: TEMCList read fEMCList write fEMCList;
  end;

  TAltEvent = class(TEvent)
  private
    function GetWhere: String; override;
    function GetSetList: String; override;
  end;

{ Details from the Flow_Event table are loaded into this object }
  TEventList = class(TMyAutoList)
  private
    function GetMedianVolume: TReal;
  protected
    function Compare(Item1,Item2: tObject): Integer; override;
  public
    procedure AssignEventIDs(Update: Boolean);
    procedure CalculateRainfallTotals(HyFile: THyFile);
    procedure CalculateFlowTotals(HyFile: THyFile);
    procedure CalculateStatistics(HyFile: THyFile; SiteDetails: TSiteDetails; All: Boolean);
    procedure CalculateEMCs(HyFile: THyFile; MedianVolume: TReal);
    procedure ExtendEvents;
    procedure LoadRecords(DB: TADODB; TableName: String; aSite: TStID);
    procedure LoadAllRecords(DB: TADODB; TableName: String; aSite: TStID);
    procedure WriteToFile(aSite: TStID; Suffix: string; Variable: TVariable);
    procedure WriteToList(Header: String; DoStatistics: Boolean);
    procedure WriteStormsToList(Header: String);
    property MedianVolume: TReal read GetMedianVolume;
  end;

{ Each record from the Sites table is loaded into a TSiteDetails object }
  TSiteDetails = class
  private
    fSite: TStID;
    fArea: TReal;
    fAreaQual: Integer;
    fHasRainEvent,fHasFlowEvent,fHasRV: Boolean;
    fFlowVar: TVariable;
    function GetArea: TReal;
  public
    constructor Create(aSite: TStID; Area: TReal; AreaQual: Integer;
      HasRainEvent,HasFlowEvent,HasRV: Boolean; FlowVar: TVariable);
    property sdSite: TStID read fSite;
    property Area: TReal read GetArea;
    property AreaQual: Integer read fAreaQual;
    property HasRainEvent: Boolean read fHasRainEvent;
    property HasFlowEvent: Boolean read fHasFlowEvent;
    property HasRV: Boolean read fHasRV;
    property FlowVar: TVariable read fFlowVar;
  end;

{ Details from the Sites table are loaded into this object }
  TSiteDetailsList = class(TMyAutoList)
  protected
    function Compare(Item1,Item2: tObject): Integer; override;
  public
    constructor Create;
    function FindSite(aSite: TStID): TSiteDetails;
    procedure LoadRecords(DB: TADODB; TableName: String);
  end;

{ Each record from the RV_Variables table is loaded into a TEventVariable object }
  TEventVariable = class
  private
    fVarNum: Integer;
    fVarCode: String;
    fDescription: String;
  public
    constructor Create(VarNum: Integer; VarCode,Description: String);
    property VarNum: Integer read fVarNum;
    property VarCode: String read fVarCode;
    property Description: String read fDescription;
  end;

{ Details from the RV_Variables table are loaded into this object }
  TEventVariableList = class(TMyAutoList)
  public
    procedure LoadRecords(DB: TADODB; TableName: String);
    function VarByCode(VarCode: String): TEventVariable;
    function VarByNum(VarNum: Integer): TEventVariable;
  end;

{ Each record from the RV_Table table is loaded into a TEventStatistic object }
  TEventStatistic = class(TTableRecord)
  private
    fID: Integer;
    fVarNum: Integer;
    fValue: TReal;
    fQuality: Integer;
    function GetFieldNames: String; override;
    function GetFieldValues: String; override;
    function GetWhere: String; override;
    function GetSetList: String; override;
    function GetDisplayText: String; override;
  public
    constructor Create(ID,Varnum: Integer);
    procedure Calculate(HyFile: THyFile; VarCode: String; Event,PrevEvent: TEvent; SiteDetails: TSiteDetails);
    property ID: Integer read fID;
    property VarNum: Integer read fVarNum;
    property Value: TReal read fValue;
    property Quality: Integer read fQuality;
  end;

  TEventStatisticList = class(TMyAutoList)
  protected
    function Compare(Item1,Item2: tObject): Integer; override;
  public
    constructor Create(ID: Integer);
    function Find(VarCode: String): TEventStatistic;
    procedure WriteToList;
  end;

  TPoint = class
  private
    fSite: TStID;
    fVariable: TVariable;
    fTime: TRelTime;
    fValue: TReal;
    fQuality: Integer;
    fComment: TStringList;
  public
    constructor Create(Site: TStID; Variable: TVariable; Time: TRelTime; Value: TReal; Quality: Integer; Comment: String);
    destructor Destroy; override;
    property Site: TStID read fSite write fSite;
    property Variable: TVariable read fVariable write fVariable;
    property Time: TRelTime read fTime;
    property Value: TReal read fValue write fValue;
    property Quality: Integer read fQuality write fQuality;
    property Comment: TStringList read fComment;
  end;

  TPointList = class(TAutoList)
  private
    fSampleType: TSampleType;
  protected
    function Compare(Item1,Item2: TObject): Integer; override;
  public
    constructor Create;
    procedure LoadWQPoints(DB: TADODB; aSite: TStID; StartTime,EndTime: TRelTime; SampleType: TSampleType);
    procedure LoadTSPoints(HyFile: THyFile; Variable: TVariable; StartTime,EndTime: TRelTime);
    procedure AddEvent(Event: TEvent);
    procedure AddPoint(Point: TPoint);
    procedure WriteToFile(aSite: TStID; Suffix: string; SampleVariable: TVariable);
  end;

{ Each record from the EMC_Variables table is loaded into a TEMCVariable object }
  TEMCVariable = class
  private
    fVariableID: Integer;
    fVariableName: String;
    fEMCVar,fLoadVar: TVariable;
    fEMCUnits,fLoadUnits,fVolumeUnits: String;
    fConversion: TReal;
  public
    constructor Create(VariableID: Integer; VariableName: String;
      EMCVar,LoadVar: TVariable; EMCUnits,LoadUnits,VolumeUnits: String; Conversion: TReal);
    property VariableID: Integer read fVariableID;
    property VariableName: String read fVariableName;
    property EMCVar: TVariable read fEMCVar;
    property LoadVar: TVariable read fLoadVar;
    property EMCUnits: String read fEMCUnits;
    property LoadUnits: String read fLoadUnits;
    property VolumeUnits: String read fVolumeUnits;
    property Conversion: TReal read fConversion;
  end;

{ Details from the EMC_Variables table are loaded into this object }
  TEMCVariableList = class(TMyAutoList)
  public
    procedure LoadRecords(DB: TADODB; TableName: String);
    function Find(Variable: TVariable): TEMCVariable;
  end;

  TEMCPartition = class(TTableRecord)
  private
    fEMC: TEMC;
    fID: Integer;
    fSampleID: String;
    fComposite: Integer;
    fVarFrom,fVarTo: TVariable;
    fStartTime,fEndTime: TRelTime;
    fCumLoad,fCumVolume,fIncLoad,fIncVolume,fStartFlowRate,fEndFlowRate,fConcentration: TReal;
    fCumLoadQuality,fCumVolumeQuality,fIncLoadQuality,fIncVolumeQuality: Integer;
    function GetKey: String;
    function GetFieldNames: String; override;
    function GetFieldValues: String; override;
    function GetFieldTypes: String; override;
    function GetDisplayText: String; override;
  public
    constructor Create(EMC: TEMC; ID: Integer; SampleID: String; Composite: Integer;
      VarFrom,VarTo: TVariable; StartTime,EndTime: TRelTime);
    procedure Calculate(HyFile: THyFile);
    property Key: String read GetKey;
  end;

  TEMCPartitionList = class(TMyAutoList)
  protected
    function Compare(Item1,Item2: TObject): Integer; override;
  public
    constructor Create;
    procedure Calculate(HyFile: THyFile);
  end;

  TEMC = class(TTableRecord)
  private
    fEvent: TEvent;
    fEMCVariable: TEMCVariable;
    fValue,fLoad,fVolume,fMaxFlowRate: TReal;
    fLoadQuality,fVolumeQuality,fNumSamples: Integer;
    fHeadVolumeScore,fInteriorVolumeScore,fTailVolumeScore: TReal;
    fHeadLoadScore,fInteriorLoadScore,fTailLoadScore: TReal;
    fVolumeScore,fLoadScore,fSampleScore,fFlowRateScore,fScore: TReal;
    fUseSampleScore,fUseVolumeScore,fComposite: Boolean;
    fErrorCode: Integer;
    fPartitionList: TEMCPartitionList;
    function GetFieldNames: String; override;
    function GetFieldValues: String; override;
    function GetFieldTypes: String; override;
    function GetDisplayText: String; override;
  public
    constructor Create(Event: TEvent; EMCVariable: TEMCVariable);
    destructor Destroy; override;
    procedure Calculate(HyFile: THyFile);
    procedure ComputeScores(HyFile: THyFile; MedianVolume: TReal);
    property PartitionList: TEMCPartitionList read fPartitionList write fPartitionList;
  end;

  TEMCList = class(TMyAutoList)
  private
  public
    function Find(EMC: String): TEMC;
    procedure LoadPartitions(HyFile: THyFile; Event: TEvent);
    procedure Calculate(HyFile: THyFile; MedianVolume: TReal);
    procedure WriteToList;
  end;

{ Each record from the Baseflow_Average table is loaded into a TBaseFlowParams object }
  TBaseFlowParams = class
  private
    fSite: TStID;
    fVariable: TVariable;
    fValue: TReal;
    fQuality: Integer;
    fPositive: Boolean;
  public
    constructor Create(Site: TStID; Variable: TVariable; Value: TReal; Quality: Integer; Positive: Boolean);
  end;

  TBaseFlowParamsList = class(TMyAutoList)
  public
    procedure LoadRecords(DB: TADODB; TableName: String);
    function Find(aSite: TStID; aVariable: TVariable): TBaseFlowParams;
  end;

var
{ Parameter screen variables }
  SiteExpr: String;
  InSuffix: string;
  LevelVarFrom,LevelVarTo: TVariable;
  FlowVarFrom,FlowVarTo: TVariable;
  VolumeVarFrom,VolumeVarTo: TVariable;
  RainVarFrom,RainVarTo: TVariable;
  IntensityVarFrom,IntensityVarTo: TVariable;
  StartTime,EndTime: TRelTime;
  PeriodOfRecord: Boolean;
  EventDBStr,WQDBStr: String;
  ProgMode: TProgMode;
  WriteTable,WriteEvent,WriteSlope: Boolean;
  EventVar,SlopeVar: TVariable;
  OutSuffix: string;
  ListDevStr: String;
  ListDev: tListDevice;

{ INI file variables }
  EventDB,WQDB: TADODB;
  TableNames: TStringList;
  SORExtendInterval: Integer;
  UpdateEventID: Boolean;

{ Global variables }
  SiteDetailsList: TSiteDetailsList;
  FlowEventParamsList: TFlowEventParamsList;
  RainEventParamsList: TRainEventParamsList;
  CombinedEventList: TEventList;
  EventVariableList: TEventVariableList;
  EMCVariableList: TEMCVariableList;
  BaseFlowParamsList: TBaseFlowParamsList;

const
  FlowStatistics = 'TQ,PQ,T_PQ,T_CQ,T_50Q';
  DateFormat     = '#MM/DD/YYYY HH.II.EE#';

procedure StartUp;
procedure DoMain;
procedure ShutDown;

implementation

uses
  HyConf, HyDatIO, HyDebugU, HyEnumSt, HyHalt, HyIniFil, HyInit, HyMath,
  HyParams, HyPutHyd, HyStnLst, HyStrLib, HyDisp, HyTimStr, HyTime, HyUCode, HyVList, HyWPrms;

{***keyword-flag*** '%v'}
const TLibVer = '50';
const Access = C+O+A+E+V+E+N+T;
const PrgName = 'COAEVENT';

{------------------------------------------------------------------------------}

procedure StartUp;
begin
  SystemStartUp(TLibVer,Access,PrgName,True);
end;

procedure ShutDown;
begin
  SystemShutDown;
end;

{------------------------------------------------------------------------------}

function RainModeToStr(RainMode: TRainMode; RainModeNum: Integer): String;
begin
  Result := GeneralTypeToStr(TypeInfo(TRainMode),Ord(RainMode),False,'rm')+
            iifStr(RainMode = rmBoth,IntToStr(RainModeNum),'');
end;

function FlowModeToStr(FlowMode: TFlowMode): String;
begin
  Result := GeneralTypeToStr(TypeInfo(TFlowMode),Ord(FlowMode),False,'fm');
end;

function ProgModeToStr(ProgMode: TProgMode): String;
begin
  Result := GeneralTypeToStr(TypeInfo(TProgMode),Ord(ProgMode),False,'pm');
end;

function SampleTypeToStr(SampleType: TSampleType): String;
begin
  Result := GeneralTypeToStr(TypeInfo(TSampleType),Ord(SampleType),False,'st');
end;

function StrToRainMode(Str: String): TRainMode;
begin
  if CharInSet (SToC(Str,FINALCHAR), ['0'..'9']) then Str := StrLeft(Str,Length(Str)-1);
  Result := TRainMode(GeneralStrToType(TypeInfo(TRainMode),Str,'rm',True,Ord(rmFlow)));
end;

function StrToFlowMode(Str: String): TFlowMode;
begin
  if StrIsNil(Str) then Str := 'None';
  Result := TFlowMode(GeneralStrToType(TypeInfo(TFlowMode),Str,'fm',True,Ord(fmLevel)));
end;

function StrToProgMode(Str: String): TProgMode;
begin
  Result := TProgMode(GeneralStrToType(TypeInfo(TProgMode),Str,'pm',True,Ord(pmCompute)));
end;

{------------------------------------------------------------------------------}

function DecRound(Value: TReal; Decimals: Integer): TReal;
begin
  Result := SToR(RToS(Value,Decimals+2,Decimals,fsFixed));
end;

{------------------------------------------------------------------------------}

function CheckTimes(HyFile: THyFile; Variable: TVariable; var StartTime,EndTime: TRelTime): Boolean;
var
  VarStartTime,VarEndTime: TRelTime;
begin
  if HyFile.TimeBounds(Variable,VarStartTime,VarEndTime) then begin
    if VarStartTime < StartTime then StartTime := VarStartTime;
    if VarEndTime > EndTime then EndTime := VarEndTime;
    Result := True;
  end else begin
    Result := False;
  end;
end;

{------------------------------------------------------------------------------}

function VariableExists(Site: TStID; Suffix: string; Variable: TVariable): Boolean;
var
  HyFile: THyFile;
  StartTime,EndTime: TRelTime;
begin
  Result := False;
  if DataSourceOK(Site,Suffix) then begin
    HyFile := THyFile.Create(Site,Suffix);
    try
      Result := CheckTimes(HyFile,Variable,StartTime,EndTime);
    finally
      HyFile.Free;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure GetValue(HyFile: THyFile; VarFrom,VarTo: TVariable;
  StartTime,EndTime: TRelTime; var Value: TReal; var Quality: Integer);
begin
  with HyFile do begin
    if StartTime <> EndTime then begin
      LoadHyValueVars(VarFrom,VarTo);
      LoadHyValueTimes(StartTime,EndTime);
      Aggregate(False);
      Value   := HyValue.Integral;
      Quality := HyValue.DaQual;
    end else if (StartTime <> 0) and (EndTime <> 0) then begin
      GetValue(VarFrom,StartTime,Value,Quality);
      VarConList.PointVarConvert(VarFrom,VarTo,StartTime,Value,Quality,Value,Quality);
    end else begin
      Value   := 0;
      Quality := ConfigRec.DefBadQual;
    end;
    Value := DecRound(Value,3);
  end;
end;

{------------------------------------------------------------------------------}

procedure GetMedian(HyFile: THyFile; VarFrom,VarTo: TVariable;
  StartTime,EndTime: TRelTime; Increment: Integer; var Value: TReal; var Quality: Integer);
var
  SortList: TMySortedValueList;
  Time: TRelTime;
  TempQual: Integer;
begin
  SortList := TMySortedValueList.Create(Round((EndTime-StartTime)/Increment),True);
  try
    Quality := ConfigRec.DefGoodQual;
    Time    := StartTime;
    while (Time <= EndTime) do begin
      HyFile.GetValue(VarFrom,Time,Value,TempQual);
      HyFile.VarConList.PointVarConvert(VarFrom,VarTo,Time,Value,TempQual,Value,TempQual);
      SortList.Add(TValueObject.Create(Value));
      Quality := Max(Quality,TempQual);
      Time := TimeOffset(Time,iMinute,Increment);
    end;

  { If all data within the period is good, return the median value }
    if Quality <= ConfigRec.MaxGoodQual then begin
      Value := DecRound(SortList.Median,3);

  { Otherwise return the instantaneous value at the middle of the period }
    end else begin
      Time := StartTime+RealToRel ((EndTime-StartTime)/2);
      GetValue(HyFile,VarFrom,VarTo,Time,Time,Value,Quality);
    end;
  finally
    SortList.Free;
  end;
end;

{------------------------------------------------------------------------------}

procedure GetMidPoint(HyFile: THyFile; VarFrom,VarTo: TVariable;
  StartTime,EndTime: TRelTime; Increment: Integer; var Value: TReal; var Quality: Integer);
var
  Total,CumValue: TReal;
  Time,StartPeriod,EndPeriod: TRelTime;
begin
  GetValue(HyFile,VarFrom,VarTo,StartTime,EndTime,Total,Quality);
  CumValue := 0;
  Time := StartTime;
  while Time < EndTime do begin
    StartPeriod := Time;
    EndPeriod   := TimeOffset (Time,iMinute,Increment);
    GetValue(HyFile,VarFrom,VarTo,StartPeriod,EndPeriod,Value,Quality);
    if Quality <= ConfigRec.MaxGoodQual then begin
      CumValue := CumValue+Value;
      if CumValue >= Total/2 then Break;
    end;
    Time := TimeOffset (Time,iMinute,Increment);
  end;
  if not Req(CumValue,Total/2) then begin
    Time := RealToRel (Interpolate(Total/2,CumValue-Value,Time-Increment,CumValue,Time,1));
  end;
  Value := Time-StartTime;
end;

{------------------------------------------------------------------------------}

procedure GetCentroid(HyFile: THyFile; VarFrom,VarTo: TVariable;
  StartTime,EndTime: TRelTime; Increment: Integer; var Value: TReal; var Quality: Integer);
var
  Total,CumValue,CumWeightedValue: TReal;
  Time,StartPeriod,EndPeriod: TRelTime;
begin
  GetValue(HyFile,VarFrom,VarTo,StartTime,EndTime,Total,Quality);
  CumValue := 0;
  CumWeightedValue := 0;
  Time := StartTime;
  while Time < EndTime do begin
    StartPeriod := Time;
    EndPeriod   := TimeOffset (Time,iMinute,Increment);
    if VarFrom = RainVarFrom { Use total in period for rainfall and value at start of period for flow }
      then GetValue(HyFile,VarFrom,VarTo,StartPeriod,EndPeriod,Value,Quality)
      else GetValue(HyFile,VarFrom,VarTo,Time,Time,Value,Quality);
    if Quality <= ConfigRec.MaxGoodQual then begin
      CumValue := CumValue+Value;
      CumWeightedValue := CumWeightedValue+(Value*(Time-StartTime));
    end;
    Time := TimeOffset (Time,iMinute,Increment);
  end;
  if not Req(CumValue,0) then Value := CumWeightedValue/CumValue;
end;

{------------------------------------------------------------------------------}

procedure CreateTable(DB: TADODB; TableName: String; TableRecord: TTableRecord);
begin
  with DB do begin
    try
      AddSQL('CREATE TABLE '+TableName+' ('+TableRecord.FieldTypes+')');
      ADOQuery.ExecSQL;
    except
      on E:Exception do begin
        if HyDebug then DispDebug('SQL',E.Message);
      end;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure DeleteTable(DB: TADODB; TableName: String; Site: TStID);
begin
  with DB do begin
    AddSQL('DELETE * FROM '+TableName+' WHERE Site = '''+StIDToS(Site)+'''');
    ADOQuery.ExecSQL;
  end;
end;

{------------------------------------------------------------------------------}

procedure ApplyUpdates(DB: TADODB);
var
  COCTable,SampleTable,EventTable,SampleView: String;
begin
  COCTable    := TableNames.Values['Chain of Custody'];        { COC }
  SampleTable := TableNames.Values['Sample Initialize Match']; { COA_SAMP }
  EventTable  := TableNames.Values['Event Output'];            { FLOW_EVENT }
  SampleView  := TableNames.Values['View COC Merge Dates'];    { View_COA_SAMP_COC }

  with DB do begin

  { Update_Event_ID_0: COC }
    AddSQL('UPDATE '+COCTable+' '+
              'SET '+COCTable+'.EVENT_ID = Null');
    ADOQuery.ExecSQL;

  { Update_Event_ID: FLOW_EVENT, COC, COA_SAMP }
    AddSQL('UPDATE ('+SampleTable+' RIGHT JOIN '+COCTable+' ON '+
                      SampleTable+'.SAMP_NO = '+COCTable+'.SAMP_NO) '+
        'INNER JOIN '+EventTable+' ON '+COCTable+'.SITE = '+EventTable+'.Site '+
               'SET '+COCTable+'.EVENT_ID = '+EventTable+'.EventID '+
          'WHERE ((('+COCTable+'.EVENT_ID) Is Null) AND '+
                    '((IIf([ALT_DATE] Is Not Null,[ALT_DATE],'+
                    'IIf([FLOW_DATE] Is Not Null,'+
                    'CDate(Format(Int(([FLOW_DATE]*2880)+0.5)/2880,"mm/dd/yyyy hh:nn:ss")),'+
                     COCTable+'.[SAMPLER_DATE])))>=[StartOfFlow] And '+
                    '(IIf([ALT_DATE] Is Not Null,[ALT_DATE],'+
                    'IIf([FLOW_DATE] Is Not Null,'+
                    'CDate(Format(Int(([FLOW_DATE]*2880)+0.5)/2880,"mm/dd/yyyy hh:nn:ss")),'+
                     COCTable+'.[SAMPLER_DATE])))<=[EndOfFlow]) AND '+
                    '(('+COCTable+'.EXCLUDE)=No) AND '+
                    '(('+EventTable+'.EMC_Event_Valid)=Yes))');
    ADOQuery.ExecSQL;

  { Update_Sampled_0: FLOW_EVENT }
    AddSQL('UPDATE '+EventTable+' '+
              'SET '+EventTable+'.Sampled = No');
    ADOQuery.ExecSQL;

  { Update_Sampled_1: FLOW_EVENT, View_COA_SAMP_COC }
    AddSQL('UPDATE '+EventTable+' INNER JOIN '+SampleView+' ON '+
                 '('+EventTable+'.EventID = '+SampleView+'.EVENT_ID) AND '+
                 '('+EventTable+'.Site = '+SampleView+'.SITE) '+
              'SET '+EventTable+'.Sampled = Yes '+
           'WHERE ('+EventTable+'.Sampled = No)');
    ADOQuery.ExecSQL;

  { Update_Sampled_2: FLOW_EVENT, View_COA_SAMP_COC }
    AddSQL('UPDATE '+EventTable+' INNER JOIN '+SampleView+' ON '+
                     EventTable+'.Site = '+SampleView+'.SITE '+
              'SET '+EventTable+'.Sampled = Yes '+
          'WHERE (('+EventTable+'.Sampled = No) AND '+
                 '('+EventTable+'.StartOfRain <= [MERGE_DATE]) AND '+
                 '('+EventTable+'.EndOfRain >= [MERGE_DATE]) AND '+
                 '('+SampleView+'.WQM_SAMPLE_ID Is Not Null) AND '+
                 '('+SampleView+'.EVENT_ID Is Null))');
    ADOQuery.ExecSQL;

  { Update_Sampled_3: FLOW_EVENT, View_COA_SAMP_COC }
    AddSQL('UPDATE '+EventTable+' INNER JOIN '+SampleView+' ON '+
                     EventTable+'.Site = '+SampleView+'.SITE '+
              'SET '+EventTable+'.Sampled = Yes '+
          'WHERE (('+EventTable+'.Sampled = No) AND '+
                 '('+EventTable+'.StartOfFlow <= [MERGE_DATE]) AND '+
                 '('+EventTable+'.EndOfFlow >= [MERGE_DATE]) AND '+
                 '('+SampleView+'.WQM_SAMPLE_ID Is Not Null) AND '+
                 '('+SampleView+'.EVENT_ID Is Null))');
    ADOQuery.ExecSQL;
  end;
end;

{------------------------------------------------------------------------------}

procedure LoadParams;
begin
  ParamRec := TParamRec.Create(PrgName);
  try
    Assrt(ParamRec.GetLine(ParamLine),'Internal Parameter Error');
    ParamLine.CheckCount(24,[],'SiteList InDataSource '+
      'LevelVarFrom LevelVarTo FlowVarFrom FlowVarTo VolumeVarFrom VolumeVarTo '+
      'RainVarFrom RainVarTo IntensityVarFrom IntensityVarTo StartTime EndTime '+
      'EventDBStr WQDBStr ProgMode WriteTable WriteEvent EventVar WriteSlope SlopeVar '+
      'OutDataSource ListDev');
    SiteExpr         := ParamLine.GetWord;
    InSuffix         := ParamLine.GetDSrc;
    LevelVarFrom     := ParamLine.GetVar;
    LevelVarTo       := ParamLine.GetVar;
    FlowVarFrom      := ParamLine.GetVar;
    FlowVarTo        := ParamLine.GetVar;
    VolumeVarFrom    := ParamLine.GetVar;
    VolumeVarTo      := ParamLine.GetVar;
    RainVarFrom      := ParamLine.GetVar;
    RainVarTo        := ParamLine.GetVar;
    IntensityVarFrom := ParamLine.GetVar;
    IntensityVarTo   := ParamLine.GetVar;
    StartTime        := ParamLine.GetTime;
    EndTime          := ParamLine.GetTime;
    PeriodOfRecord   := TimesEQ (StartTime,EndTime);
    EventDBStr       := ParamLine.GetWord;
    WQDBStr          := ParamLine.GetWord;
    ProgMode         := StrToProgMode(ParamLine.GetWord);
    WriteTable       := ParamLine.GetBool;
    WriteEvent       := ParamLine.GetBool;
    EventVar         := ParamLine.GetVar;
    WriteSlope       := ParamLine.GetBool;
    SlopeVar         := ParamLine.GetVar;
    OutSuffix        := ParamLine.GetDSrc;
    ListDevStr       := ParamLine.GetStr;
  finally
    ParamRec.Free;
  end;
end;

{------------------------------------------------------------------------------}

procedure LoadIniFile;
var
  IniFile: THydIniFile;
  ConnectionStr: TStringList;
begin
  IniFile       := THydIniFile.Create('COAEVENT.INI',2);
  ConnectionStr := TStringList.Create;
  TableNames    := TStringList.Create;
  try
    IniFile.LoadSection('Table Names',TableNames);
    IniFile.LoadSection('Event Database',ConnectionStr);
    ConnectionStr.Values['Data Source'] := EventDBStr;
    EventDB := TADODB.Create(StrListToStr(ConnectionStr,';'));
    if ProgMode = pmEMC then begin
      IniFile.LoadSection('WQ Database',ConnectionStr);
      ConnectionStr.Values['Data Source'] := WQDBStr;
      WQDB := TADODB.Create(StrListToStr(ConnectionStr,';'));
    end;
    SORExtendInterval := IniFile.GetInt('Options','Start of Rain Extend Interval',0);
    UpdateEventID := IniFile.GetBool   ('Options','Update Event ID',True);
  finally
    IniFile.Free;
    ConnectionStr.Free;
  end;
end;

{------------------------------------------------------------------------------}

procedure ComputeRecessionSlope(aSite: TStID; StartTime,EndTime: TRelTime);
const
  MissingQual  = 201;
var
  OutFile: TPutHyd;
  FlowEventParams: TFlowEventParams;
  PointList: TAutoList;
  SortList: TMySortedValueList;
  MedianSuffix,InterpSuffix,SlopeSuffix: string;
  FlowFile,MedianFile,InterpFile: THyFile;
  Buffer: array[1..4] of TDataPoint;
  Time,StartPeriod,EndPeriod,InterpTime: TRelTime;
  Value,PrevValue,InterpValue,Median,FlowRatio,Slope: TReal;
  Quality,PrevQuality,PointNum,StartIndex,EndIndex,i: Integer;
begin
  if not VariableExists(aSite,OutSuffix,SlopeVar) then begin
    FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(aSite,StartTime));
    if FlowEventParams.Method = fmBFS then begin
      Assrt(FlowEventParams.DropInterval <> 0,
        'Unable to compute recession slope for site '+StIDToS(aSite)+'. '+
        TableNames.Values['Flow Event Parameters']+'.Drop_Periods is zero.');
      OutFile := TPutHyd.Create(tswmAppend,0,phPut);
      try

      { Pass 1: Median filter }
        MedianSuffix := DataSourceSpecList.GenerateTempTSFileDataSource (aSite);
        PointList := TAutoList.Create(TimeIncCount (StartTime, EndTime, iMinute), False, True);
        SortList  := TMySortedValueList.Create(FlowEventParams.DropPeriod+1,True);
        FlowFile  := THyFile.Create(aSite,InSuffix);
        with FlowFile do try
          Time := StartTime;
          LoadHyValueVars(FlowVarFrom,FlowVarTo);
          while Time < EndTime do begin

          { Determine period }
            FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(aSite,Time));
            StartPeriod := Time;
            EndPeriod   := TimeOffset(Time,iMinute,FlowEventParams.SiteInterval);

          { Read flow for period }
            LoadHyValueTimes(StartPeriod,EndPeriod);
            Aggregate(False);
            Value := HyValue.VStart;

          { Add value to list }
            PointList.Add(TValueObject.Create(Value));
            Time := TimeOffset (Time,iMinute,FlowEventParams.SiteInterval);
            if DispDelay(1) then Disp('Applying median filter: '+StIdToS(aSite)+' '+InSuffix+' '+VariableAsString(FlowVarTo)+' '+TToS(Time,True));
          end;

          Time := StartTime;
          PointNum := 0;
          while Time <= EndTime do begin

          { Determine filter window }
            FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(aSite,Time));
            StartIndex := PointNum-Trunc(FlowEventParams.DropPeriod/2);
            EndIndex   := PointNum+Trunc(FlowEventParams.DropPeriod/2);
            if StartIndex < 0 then StartIndex := 0;
            if EndIndex > PointList.Count-1 then EndIndex := PointList.Count-1;

          { Write median value to output file }
            SortList.Clear;
            for i := StartIndex to EndIndex do SortList.AddValue(TValueObject(PointList.Items[i]).Val);
            Median := DecRound(SortList.Median,2);
            OutFile.AddPoint(aSite,MedianSuffix,FlowVarTo,Time,Median,1,dtrInst);

            Time := TimeOffset (Time,iMinute,FlowEventParams.SiteInterval);
            PointNum := PointNum+1;
            if DispDelay(1) then Disp('Writing filtered data: '+StIdToS(aSite)+' '+MedianSuffix+' '+VariableAsString(FlowVarTo)+' '+TToS(Time,True));
          end;
          OutFile.NewFile;
        finally
          PointList.Free;
          SortList.Free;
          FlowFile.Free;
        end;

      { Pass 2: Interpolate in log space }
        InterpSuffix := DataSourceSpecList.GenerateTempTSFileDataSource (aSite);
        MedianFile := THyFile.Create(aSite,MedianSuffix);
        InterpTime := NullTime; // avoid compiler warning
        with MedianFile do try
          Time := StartTime-relSecond;
          GetValue(FlowVarTo,Time,Value,Quality);
          NextValue(FlowVarTo,Time,Value,Quality);
          while Time <= EndTime do begin
            FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(aSite,Time));

          { Shuffle buffer }
            Buffer[1] := Buffer[2];
            Buffer[2] := Buffer[3];
            Buffer[3] := Buffer[4];

          { Add new point to end of buffer }
            Buffer[4].Time    := Time;
            Buffer[4].Value   := Value;
            Buffer[4].Quality := Quality;

            if Buffer[1].Time > 0 then begin
              with Buffer[1] do OutFile.AddPoint(aSite,InterpSuffix,FlowVarTo,Time,Value,Quality,dtrInst);

            { Interpolate when Value1 > Value2 = Value3 > Value4 }
              if (Buffer[1].Value > Buffer[2].Value) and
                 Req(Buffer[2].Value,Buffer[3].Value) and
                 (Buffer[3].Value > Buffer[4].Value) then begin
                InterpTime := Buffer[2].Time;
                while InterpTime <= Buffer[3].Time do begin
                  InterpValue := DecRound(Interpolate(InterpTime,
                    Buffer[2].Time,(Buffer[1].Value+Buffer[2].Value)/2,
                    Buffer[3].Time,(Buffer[3].Value+Buffer[4].Value)/2,2),3);
                  OutFile.AddPoint(aSite,InterpSuffix,FlowVarTo,InterpTime,InterpValue,1,dtrInst);
                  InterpTime := TimeOffset (InterpTime,iMinute,FlowEventParams.SiteInterval);
                end;
              end;
            end;
            NextValue(FlowVarTo,Time,Value,Quality);
            if DispDelay(1) then Disp('Writing interpolated data: '+StIdToS(aSite)+' '+InterpSuffix+' '+VariableAsString(FlowVarTo)+' '+TToS(InterpTime,True));
          end;

        { Write final 3 points }
          with Buffer[2] do if Time > 0 then
            OutFile.AddPoint(aSite,InterpSuffix,FlowVarTo,Time,Value,Quality,dtrInst);
          with Buffer[3] do if Time > 0 then
            OutFile.AddPoint(aSite,InterpSuffix,FlowVarTo,Time,Value,Quality,dtrInst);
          with Buffer[4] do if Time > 0 then
            OutFile.AddPoint(aSite,InterpSuffix,FlowVarTo,Time,Value,Quality,dtrInst);
          OutFile.NewFile;
        finally
          MedianFile.Free;
        end;

      { Pass 3: Compute recession slope }
        SlopeSuffix := OutSuffix;
        InterpFile  := THyFile.Create(aSite,InterpSuffix);
        try
          Time := StartTime;
          while Time < EndTime do begin
            FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(aSite,Time));

          { Compute average flow for previous interval }
            StartPeriod := TimeOffset (Time, iMinute, -FlowEventParams.DropInterval);
            EndPeriod   := Time;
            if StartPeriod < StartTime then StartPeriod := StartTime;
            if EndPeriod > EndTime then EndPeriod := EndTime;
            GetValue(InterpFile,FlowVarTo,FlowVarTo,StartPeriod,EndPeriod,PrevValue,PrevQuality);

          { Compute average flow for next interval }
            StartPeriod := Time;
            EndPeriod   := TimeOffset (Time, iMinute, +FlowEventParams.DropInterval);
            if StartPeriod < StartTime then StartPeriod := StartTime;
            if EndPeriod > EndTime then EndPeriod := EndTime;
            GetValue(InterpFile,FlowVarTo,FlowVarTo,StartPeriod,EndPeriod,Value,Quality);

          { Compute flow ratio }
            if (Value > 0) and (PrevValue > 0) then begin
              FlowRatio := Value/PrevValue;
            end else if Req(Value,0) then begin
              FlowRatio := 1;
            end else begin
              FlowRatio := -1;
            end;

          { Compute recession slope }
            if FlowRatio <> -1 then begin
              Slope := Exp(Ln(FlowRatio)*(1440/FlowEventParams.DropInterval));
              if Slope < 0.1 then begin
                Slope   := DecRound(Slope,3);
                Quality := Max(Quality,PrevQuality);
              end else if Slope <= 1.1 then begin
                Slope   := DecRound(Slope,2);
                Quality := Max(Quality,PrevQuality);
              end else begin
                Slope   := 0;
                Quality := MissingQual;
              end;
            end else begin
              Slope   := 0;
              Quality := MissingQual;
            end;
            OutFile.AddPoint(aSite,SlopeSuffix,SlopeVar,Time,Slope,Quality,dtrInst);
            Time := TimeOffset (Time,iMinute,FlowEventParams.SiteInterval);
            if DispDelay(1) then Disp('Writing recession slope: '+StIdToS(aSite)+' '+SlopeSuffix+' '+VariableAsString(SlopeVar)+' '+TToS(Time,True));
          end;
          OutFile.NewFile;
        finally
          InterpFile.Free;
        end;
      finally
        DeleteTSFile(aSite,MedianSuffix,False);
        DeleteTSFile(aSite,InterpSuffix,False);
        OutFile.Free;
      end;
    end;
  end else begin
    DispPush([dspWarning]);
    try
      Disp('Recession slope trace (variable '+VariableAsString(SlopeVar)+') '+
        'already exists in '+StIDToS(aSite)+'.'+OutSuffix);
    finally
      DispPop;
    end;
  end;
end;

{------------------------------------------------------------------------------}

function AdjustStartTime(HyFile: THyFile; FlowEventParams: TFlowEventParams; MinBaseFlow: TDataPoint): TRelTime;
var
  Time: TRelTime;
  InstFlow: TDataPoint;
begin
  Time := MinBaseFlow.Time;
  with FlowEventParams do begin
    GetValue(HyFile,FlowVarFrom,FlowVarTo,Time+SiteInterval,Time+SiteInterval,InstFlow.Value,InstFlow.Quality);
    while InstFlow.Value <= MinBaseFlow.Value do begin
      Time := TimeOffset (Time,iMinute,SiteInterval);
      GetValue(HyFile,FlowVarFrom,FlowVarTo,Time+SiteInterval,Time+SiteInterval,InstFlow.Value,InstFlow.Quality);
    end;
    GetValue(HyFile,FlowVarFrom,FlowVarTo,Time,Time,InstFlow.Value,InstFlow.Quality);
    while InstFlow.Value > MinBaseFlow.Value do begin
      Time := TimeOffset (Time,iMinute,-SiteInterval);
      GetValue(HyFile,FlowVarFrom,FlowVarTo,Time,Time,InstFlow.Value,InstFlow.Quality);
    end;
  end;
  Result := Time;
end;

{------------------------------------------------------------------------------}

function AdjustEndTime(HyFile,SlopeHyFile: THyFile; FlowEventParams: TFlowEventParams;
  MinBaseFlow,PeakFlow: TDataPoint; EndTime: TRelTime; Search: Boolean): TRelTime;
var
  Time: TRelTime;
  MedianFlow,InstFlow,PrevInstFlow,NextInstFlow: TDataPoint;
  RecessionSlope,MinSlope: TDataPoint;
  TestFlow: TReal;
  FoundEnd,Adjust: Boolean;
begin
  Time := EndTime;
  FoundEnd := True;
  with FlowEventParams do begin
    if Search then begin
      GetMedian(HyFile,FlowVarFrom,FlowVarTo,
        TimeOffset (Time, iMinute, -RiseInterval/2),
        TimeOffset (Time, iMinute, +RiseInterval/2),
        SiteInterval,MedianFlow.Value,MedianFlow.Quality);
      GetValue(SlopeHyFile,SlopeVar,SlopeVar,Time,Time,RecessionSlope.Value,RecessionSlope.Quality);

    { Search backwards }
      if (MedianFlow.Value < MinBaseFlow.Value) or (RecessionSlope.Value > RecessionThreshold) then begin
        MinSlope := RecessionSlope;
        if (MinSlope.Value < 0) or (MinSlope.Value > 1) then begin
          MinSlope.Value := 1;
        end;

        Time := TimeOffset (Time,iMinute,-SiteInterval);
        GetMedian(HyFile,FlowVarFrom,FlowVarTo,
          TimeOffset (Time, iMinute, -RiseInterval/2),
          TimeOffset (Time, iMinute, +RiseInterval/2),
          SiteInterval,MedianFlow.Value,MedianFlow.Quality);
        GetValue(SlopeHyFile,SlopeVar,SlopeVar,Time,Time,RecessionSlope.Value,RecessionSlope.Quality);
        while ((MedianFlow.Value < MinBaseFlow.Value) or (RecessionSlope.Value > RecessionThreshold)) and (Time > PeakFlow.Time) do begin
          if (RecessionSlope.Value >= 0) and (MinSlope.Value > RecessionSlope.Value) then begin
            MinSlope := RecessionSlope;
          end;
          Time := TimeOffset (Time,iMinute,-SiteInterval);
          GetMedian(HyFile,FlowVarFrom,FlowVarTo,
            TimeOffset (Time, iMinute, -RiseInterval/2),
            TimeOffset (Time, iMinute, +RiseInterval/2),
            SiteInterval,MedianFlow.Value,MedianFlow.Quality);
          GetValue(SlopeHyFile,SlopeVar,SlopeVar,Time,Time,RecessionSlope.Value,RecessionSlope.Quality);
        end;

        if MedianFlow.Value < MinBaseFlow.Value then begin
          FoundEnd := False; { Median flow rate at end of flow < minimum baseflow }
        end else if RecessionSlope.Value > RecessionThreshold then begin
          while (MedianFlow.Value > MinBaseFlow.Value) and (Time < MinSlope.Time) do begin
            Time := TimeOffset (Time,iMinute,SiteInterval);
            GetMedian(HyFile,FlowVarFrom,FlowVarTo,
              TimeOffset (Time, iMinute, -RiseInterval/2),
              TimeOffset (Time, iMinute, +RiseInterval/2),
              SiteInterval,MedianFlow.Value,MedianFlow.Quality);
          end;
          if not TimesEQ (Time,MinSlope.Time) or (RecessionSlope.Value >= 1) then begin
            FoundEnd := False { Minimum slope was not found }
          end;
        end;

    { Search forwards }
      end else begin
        while (MedianFlow.Value > MinBaseFlow.Value) and (RecessionSlope.Value < RecessionThreshold) and (Time < EndTime) do begin
          Time := TimeOffset (Time,iMinute,SiteInterval);
          GetMedian(HyFile,FlowVarFrom,FlowVarTo,
            TimeOffset (Time, iMinute, -RiseInterval/2),
            TimeOffset (Time, iMinute, +RiseInterval/2),
            SiteInterval,MedianFlow.Value,MedianFlow.Quality);
          GetValue(SlopeHyFile,SlopeVar,SlopeVar,Time,Time,RecessionSlope.Value,RecessionSlope.Quality);
        end;
        if TimesEQ (Time,EndTime) then begin
          FoundEnd := False;
        end;
      end;
    end;

    if FoundEnd then begin
      GetMedian(HyFile,FlowVarFrom,FlowVarTo,
        TimeOffset (Time, iMinute, -RiseInterval/2),
        TimeOffset (Time, iMinute, +RiseInterval/2),
        SiteInterval,MedianFlow.Value,MedianFlow.Quality);
      TestFlow := MedianFlow.Value;
      if TestFlow < MinBaseFlow.Value then begin
        TestFlow := MinBaseFlow.Value;
      end;
      Adjust := False;

      GetValue(HyFile,FlowVarFrom,FlowVarTo,Time,Time,InstFlow.Value,InstFlow.Quality);
      GetValue(HyFile,FlowVarFrom,FlowVarTo,
        TimeOffset (Time, iMinute, -SiteInterval),
        TimeOffset (Time, iMinute, -SiteInterval),
        PrevInstFlow.Value,PrevInstFlow.Quality);
      while (((InstFlow.Value >= PrevInstFlow.Value) or
            (InstFlow.Value <= TestFlow) or
            (InstFlow.Quality > ConfigRec.MaxGoodQual)) and
            (Time > PeakFlow.Time)) do begin
        Adjust := True;
        Time := TimeOffset (Time,iMinute,-SiteInterval);
        GetValue(HyFile,FlowVarFrom,FlowVarTo,Time,Time,InstFlow.Value,InstFlow.Quality);
        GetValue(HyFile,FlowVarFrom,FlowVarTo,
          TimeOffset (Time, iMinute, -SiteInterval),
          TimeOffset (Time, iMinute, -SiteInterval),
          PrevInstFlow.Value,PrevInstFlow.Quality);
      end;

      GetValue(HyFile,FlowVarFrom,FlowVarTo,Time,Time,InstFlow.Value,InstFlow.Quality);
      GetValue(HyFile,FlowVarFrom,FlowVarTo,
        TimeOffset (Time, iMinute, +SiteInterval),
        TimeOffset (Time, iMinute, +SiteInterval),
        NextInstFlow.Value,NextInstFlow.Quality);
      if NextInstFlow.Quality <= ConfigRec.MaxGoodQual then begin
        if TimesEQ (Time,PeakFlow.Time) then begin
          Time := TimeOffset (Time,iMinute,SiteInterval);
        end else if Adjust {and (NextInstFlow.Quality <= ConfigRec.MaxGoodQual)} and
          (NextInstFlow.Value < InstFlow.Value) then begin
          Time := TimeOffset (Time,iMinute,SiteInterval);
        end;
      end;
      Result := Time;
    end else begin
      Result := EndTime;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure ExtractFlowEventsOld(HyFile: THyFile; StartTime,EndTime: TRelTime; EventList: TEventList; RemoveEvents: Boolean);
var
  FlowEventParams: TFlowEventParams;
  Time,TempTime,StartPeriod,EndPeriod: TRelTime;
  Value: TReal;
  Quality,PrevQuality,i: Integer;
  InEvent,Above: Boolean;
  Event: TEvent;
begin
  InEvent := False;
  Time    := StartTime;
  PrevQuality := ConfigRec.DefBadQual;
  with HyFile do begin
    while Time < EndTime do begin

    { Determine period }
      FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(HSite,Time));
      StartPeriod := Time;
      EndPeriod   := TimeOffset (Time,iMinute,FlowEventParams.SiteInterval);

    { Read level or flow for period }
      case FlowEventParams.Method of
        fmLevel: LoadHyValueVars(LevelVarFrom,LevelVarTo);
        fmFlow:  LoadHyValueVars(FlowVarFrom,FlowVarTo);
        fmBFS:   Abort('Baseflow separation method not implemented. Site '+StIdToS(HSite)+', time '+TToS(Time,False));
      end;
      LoadHyValueTimes(StartPeriod,EndPeriod);
      Aggregate(False);
      Value   := HyValue.VStart;
      Quality := HyValue.DaQual;
      Value   := SToR(RToS(Value,5,3,fsFixed)); { Round value to 3 decimal places }

    { Is this period above the threshold? }
      Above := False;
      if (Quality <= ConfigRec.MaxGoodQual) then begin
        case FlowEventParams.Method of
          fmLevel: Above := Value > FlowEventParams.LevelThreshold;
          fmFlow:  Above := Value > FlowEventParams.FlowThreshold;
          fmBFS:   Abort('Baseflow separation method not implemented. Site '+StIdToS(HSite)+', time '+TToS(Time,False));
        end;
      end;

    { Assign period to new/existing event if above the threshold }
      if Above then begin
        if InEvent then begin
          Event := TEvent(EventList.Items[EventList.Count-1]);
          Event.EndOfFlow := EndPeriod;
          if Quality > Event.FlowQual then Event.FlowQual := Quality;
        end else begin
          Event := TEvent.Create(HSite,InSuffix,0);
          Event.FlowMode    := FlowEventParams.Method;
          Event.StartOfFlow := StartPeriod;
          Event.EndOfFlow   := EndPeriod;
          Event.FlowQual    := Quality;
          Event.RainMode    := rmFlow;
          EventList.Add(Event);

        { Move start of event back one period to catch all level/flow above the threshold }
          TempTime := TimeOffset(Event.StartOfFlow,iMinute,-FlowEventParams.SiteInterval);
          if (TempTime >= FlowEventParams.StartDate) and (PrevQuality <= ConfigRec.MaxGoodQual) then begin
            Event.StartOfFlow := TempTime;
          end;
        end;
      end;
      InEvent := Above;
      PrevQuality := Quality;

      Time := TimeOffset (Time,iMinute,FlowEventParams.SiteInterval);
      if DispDelay(1) then Disp('Extracting flow events: '+StIdToS(HSite)+' '+InSuffix+' '+VariableAsString(LevelVarFrom)+' '+TToS(Time,True));
    end;

  { Compute total volumes }
    EventList.CalculateFlowTotals(HyFile);

  { Remove events which do not exceed the time threshold }
    if RemoveEvents then begin
      for i := EventList.Count-1 downto 0 do begin
        with TEvent(EventList.Items[i]) do begin
          FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(HSite,StartOfFlow));
          if EndOfFlow-StartOfFlow < FlowEventParams.TimeThreshold then EventList.Delete(i);
        end;
      end;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure ExtractFlowEvents(HyFile: THyFile; StartTime,EndTime: TRelTime; EventList: TEventList; RemoveEvents: Boolean);

  procedure Init(var DataPoint: TDataPoint; Value: TReal);
  begin
    DataPoint.Time    := 0;
    DataPoint.Value   := Value;
    DataPoint.Quality := 255;
  end;

var
  FlowEventParams: TFlowEventParams;
  Time,TempTime,StartPeriod,EndPeriod: TRelTime;
  Value: TReal;
  Quality,PrevQuality,i: Integer;
  InEvent,Above: Boolean;
  Event: TEvent;
  Site: TStID;

  SlopeHyFile: THyFile;
  EndDelayPeriod: TRelTime;
  MedianFlow,AverageFlow,InstFlow,RecessionSlope: TDataPoint;
  MinBaseFlow,MinStormFlow,PeakFlow: TDataPoint;
  InitBaseFlow,InitStormFlow: Boolean;
begin
{ Using a separate HyFile to read the slope gives much better performance }
  Site        := HyFile.HSite;
  SlopeHyFile := THyFile.Create(Site,InSuffix);
  try
    Event          := nil;
    InEvent        := False;
    Time           := StartTime;
    EndDelayPeriod := StartTime;
    PrevQuality    := ConfigRec.DefBadQual;
    InitBaseFlow   := True;
    InitStormFlow  := True;
    Init(MinBaseFlow,HUGE);
    Init(MinStormFlow,HUGE);
    Init(PeakFlow,-HUGE);
    while Time < EndTime do begin
      FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(Site,Time));
      with FlowEventParams do begin

      { Determine period }
        StartPeriod := Time;
        EndPeriod   := TimeOffset (Time,iMinute,SiteInterval);

        if Method in [fmLevel,fmFlow] then begin

        { Read level or flow for period }
          with HyFile do begin
            if Method = fmLevel
              then LoadHyValueVars(LevelVarFrom,LevelVarTo)
              else LoadHyValueVars(FlowVarFrom,FlowVarTo);
            LoadHyValueTimes(StartPeriod,EndPeriod);
            Aggregate(False);
            Value   := DecRound(HyValue.VStart,3);
            Quality := HyValue.DaQual;
          end;

        { Is this period above the threshold? }
          Above := False;
          if (Quality <= ConfigRec.MaxGoodQual) then begin
            if Method = fmLevel
              then Above := Value > LevelThreshold
              else Above := Value > FlowThreshold;
          end;

        { Assign period to new/existing event if above the threshold }
          if Above then begin
            if InEvent then begin
              Event.EndOfFlow := EndPeriod;
              if Quality > Event.FlowQual then Event.FlowQual := Quality;
            end else begin
              Event := TEvent.Create(Site,InSuffix,0);
              Event.FlowMode    := Method;
              Event.StartOfFlow := StartPeriod;
              Event.EndOfFlow   := EndPeriod;
              Event.FlowQual    := Quality;
              Event.RainMode    := rmFlow;
              EventList.Add(Event);

            { Move start of event back one period to catch all level/flow above the threshold }
              TempTime := TimeOffset (Event.StartOfFlow,iMinute,-SiteInterval);
              if (TempTime >= StartDate) and (PrevQuality <= ConfigRec.MaxGoodQual) then begin
                Event.StartOfFlow := TempTime;
              end;
            end;
          end;
          InEvent := Above;
          PrevQuality := Quality;

        end else begin

        { Read median, average, instantaneous flow and recession slope for period }
          MedianFlow.Time     := Time;
          AverageFlow.Time    := Time;
          InstFlow.Time       := Time;
          RecessionSlope.Time := Time;
          GetMedian(HyFile,FlowVarFrom,FlowVarTo,
            TimeOffset (Time, iMinute, -RiseInterval/2),
            TimeOffset (Time, iMinute, +RiseInterval/2),
            SiteInterval,MedianFlow.Value,MedianFlow.Quality);
          GetValue(HyFile,FlowVarFrom,FlowVarTo,
            Time,
            TimeOffset (Time, iMinute, +RiseInterval),
            AverageFlow.Value,AverageFlow.Quality);
          GetValue(HyFile,FlowVarFrom,FlowVarTo,Time,Time,InstFlow.Value,InstFlow.Quality);
          GetValue(SlopeHyFile,SlopeVar,SlopeVar,Time,Time,RecessionSlope.Value,RecessionSlope.Quality);

        { Recession slope is not valid when it is greater than 1 }
          if (RecessionSlope.Value > 1) or (RecessionSlope.Quality > ConfigRec.MaxGoodQual) then begin
            RecessionSlope.Value := -1;
          end;

          if (MedianFlow.Quality <= ConfigRec.MaxGoodQual) {and}or
             (AverageFlow.Quality <= ConfigRec.MaxGoodQual) then begin

            {ListDev.Write(TToS(Time,True)+','+
              RToS(MedianFlow.Value,5,3,fsFixed)+','+
              RToS(AverageFlow.Value,5,3,fsFixed)+','+
              RToS(InstFlow.Value,5,3,fsFixed)+','+
              RToS(RecessionSlope.Value,5,3,fsFixed)+',');}

          { Scan storm flow }
            if InEvent then begin

            { Initialise storm flow scanning }
              if InitStormFlow then begin
                MinStormFlow   := MedianFlow;
                PeakFlow       := AverageFlow;
                EndDelayPeriod := TimeOffset (Time, iMinute, +DelayInterval);
                InitStormFlow  := False;
              end;

            { Update event quality }
              if AverageFlow.Quality > Event.FlowQual then begin
                Event.FlowQual := AverageFlow.Quality;
              end;

            { Update minimum storm flow }
              if MinStormFlow.Value >= MedianFlow.Value then begin
                MinStormFlow := MedianFlow;
              end;
              if (AverageFlow.Value-MinStormFlow.Value) >= FlowThreshold-Epsilon then begin
                MinStormFlow   := MedianFlow;
                PeakFlow       := AverageFlow;
                EndDelayPeriod := TimeOffset (Time, iMinute, +DelayInterval);
              end;
              if PeakFlow.Value <= InstFlow.Value then begin
                MinStormFlow   := MedianFlow;
                PeakFlow       := InstFlow;
                EndDelayPeriod := TimeOffset (Time, iMinute, +DelayInterval);
              end;

            { End of storm has been detected }
              if (MedianFlow.Value < MinBaseFlow.Value) or
                 ((Time >= EndDelayPeriod) and Req(MedianFlow.Value,MinBaseFlow.Value)) or
                 ((Time >= EndDelayPeriod) and (RecessionSlope.Value >= RecessionThreshold)) then begin
                Time := AdjustEndTime(HyFile,SlopeHyFile,FlowEventParams,MinBaseFlow,PeakFlow,Time,not RemoveEvents);
                Event.EndOfFlow := Time;
                InEvent         := False;
                InitBaseFlow    := True;
              end else if MedianFlow.Quality > ConfigRec.MaxGoodQual then begin
                Event.EndOfFlow := Time+SiteInterval;
                InEvent         := False;
                InitBaseFlow    := True;
              end;

          { Scan baseflow }
            end else begin

            { Initialise baseflow scanning }
              if InitBaseFlow then begin
                MinBaseFlow  := MedianFlow;
                InitBaseFlow := False;
              end;

            { If median flow is bad quality, reset minimum baseflow }
              if MedianFlow.Quality >= ConfigRec.MaxGoodQual then begin
                MinBaseFlow := MedianFlow;
              end;

            { Update minimum baseflow }
              if MinBaseFlow.Value >= MedianFlow.Value then begin
                MinBaseFlow := MedianFlow;

            { Recompute minimum baseflow }
              end else if (Time-MinBaseFlow.Time) > StartExtendInterval then begin
                Init(MinBaseFlow,HUGE);
                TempTime := TimeOffset (Time, iMinute, -StartExtendInterval);
                while TempTime <= Time do begin
                  MedianFlow.Time := TempTime;
                  GetMedian(HyFile,FlowVarFrom,FlowVarTo,
                    TimeOffset (TempTime, iMinute, -RiseInterval/2),
                    TimeOffset (TempTime, iMinute, +RiseInterval/2),
                    SiteInterval,MedianFlow.Value,MedianFlow.Quality);
                  if MinBaseFlow.Value >= MedianFlow.Value then begin
                    MinBaseFlow := MedianFlow;
                  end;
                  TempTime := TimeOffset (TempTime,iMinute,SiteInterval);
                end;
              end;

            { Start of storm has been detected }
              if (AverageFlow.Quality <= ConfigRec.MaxGoodQual) and (MinBaseFlow.Quality <= ConfigRec.MaxGoodQual) then begin
                if (AverageFlow.Value-MinBaseFlow.Value) >= FlowThreshold-Epsilon then begin
                  Time := AdjustStartTime(HyFile,FlowEventParams,MinBaseFlow);
                  if Assigned(Event) and (Time < Event.EndOfFlow) then Time := Event.EndOfFlow;
                  Event := TEvent.Create(Site,InSuffix,0);
                  Event.FlowMode    := Method;
                  Event.StartOfFlow := Time;
                  Event.EndOfFlow   := Event.StartOfFlow;
                  Event.FlowQual    := AverageFlow.Quality;
                  Event.RainMode    := rmFlow;
                  EventList.Add(Event);
                  InEvent           := True;
                  InitStormFlow     := True;
                end;
              end;

            end;

            {ListDev.Writeln(
              RToS(MinBaseFlow.Value,5,3,fsFixed)+','+
              InttoStr(MinBaseFlow.Quality)+','+
              RToS(MinStormFlow.Value,5,3,fsFixed)+','+
              RToS(PeakFlow.Value,5,3,fsFixed)+','+
              iifStr(InEvent,'Yes','No'));}

          end else begin

          { End of storm has been detected }
            if InEvent then begin
              Time := AdjustEndTime(HyFile,SlopeHyFile,FlowEventParams,MinBaseFlow,PeakFlow,Time,not RemoveEvents);
              Event.EndOfFlow := Time;
            end;
            InEvent         := False;
            InitBaseFlow    := True;
          end;
        end;

        Time := TimeOffset (Time,iMinute,SiteInterval);
        if DispDelay(1) then Disp('Extracting flow events: '+StIdToS(Site)+' '+InSuffix+' '+VariableAsString(LevelVarFrom)+' '+TToS(Time,True));
      end;
    end;
   if InEvent then Event.EndOfFlow := Time;

  { Remove events which do not exceed the time threshold }
    if RemoveEvents then begin
      for i := EventList.Count-1 downto 0 do begin
        with TEvent(EventList.Items[i]) do begin
          FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(Site,StartOfFlow));
          if EndOfFlow-StartOfFlow < FlowEventParams.TimeThreshold then EventList.Delete(i);
        end;
      end;
    end;
  finally
    SlopeHyFile.Free;
  end;
end;

{------------------------------------------------------------------------------}

procedure ExtractRainEvents(HyFile: THyFile; StartTime,EndTime: TRelTime; EventList: TEventList; RemoveEvents: Boolean);
var
  RainEventParams: TRainEventParams;
  Time,PrevTime,TempTime,StartPeriod,EndPeriod: TRelTime;
  Rain,Intensity: TReal;
  RainQual,IntensityQual,PrevQual,i: Integer;
  InEvent: Boolean;
  Event: TEvent;

  procedure AddEvent(StartPeriod,EndPeriod: TRelTime);
  begin
    Event := TEvent.Create(HyFile.HSite,InSuffix,0);
    Event.StartOfRain := StartPeriod;
    Event.EndOfRain   := EndPeriod;
    Event.RainMode    := rmRain;
    EventList.Add(Event);
    InEvent := True;

  { Move start of event back one period to catch all rainfall }
    TempTime := TimeOffset (Event.StartOfRain,iMinute,-RainEventParams.SiteInterval);
    if (TempTime >= RainEventParams.StartDate) and (TempTime >= StartTime) and (PrevQual <= ConfigRec.MaxGoodQual) then begin
      Event.StartOfRain := TempTime;
    end;
  end;

begin
  InEvent  := False;
  Time     := StartTime;
  HyFile.GetValue(RainVarFrom,Time,Rain,RainQual);
  PrevTime := Time;
  PrevQual := RainQual;
  while Time < EndTime do begin

    HyFile.NextValue(RainVarFrom,Time,Rain,RainQual);
    if (Rain > 0) and (RainQual <= ConfigRec.MaxGoodQual) then begin
      RainEventParams := TRainEventParams(RainEventParamsList.Find(HyFile.HSite,Time));

      if not InEvent then begin

      { Read rainfall for period }
        StartPeriod := Time-RainEventParams.SiteInterval;
        EndPeriod   := Time;
        GetValue(HyFile,RainVarFrom,RainVarTo,StartPeriod,EndPeriod,Rain,RainQual);

      { Is this the start of a new event? }
        if (Rain >= RainEventParams.MinIntervalTotal) and (RainQual <= ConfigRec.MaxGoodQual) then begin
          AddEvent(StartPeriod,EndPeriod);

        end else begin

        { Compute intensity for period }
          StartPeriod := PrevTime;
          EndPeriod   := Time;
          GetValue(HyFile,IntensityVarFrom,IntensityVarTo,StartPeriod,EndPeriod,Intensity,IntensityQual);

        { Is this the start of a new event? }
          if (Intensity >= RainEventParams.DrizzleRate) and (IntensityQual <= ConfigRec.MaxGoodQual) then begin
            AddEvent(StartPeriod,EndPeriod);
          end;
        end;

      end else begin
        Event := TEvent(EventList.Items[EventList.Count-1]);

      { Compute intensity between end of current event and new point }
        GetValue(HyFile,IntensityVarFrom,IntensityVarTo,Event.EndOfRain,Time,Intensity,IntensityQual);

      { Has the drizzle rate been exceeded? }
        if (Intensity >= RainEventParams.DrizzleRate) and (IntensityQual <= ConfigRec.MaxGoodQual) then begin
          Event.EndOfRain := Time;
        end else begin
          InEvent := Time-Event.EndOfRain < RainEventParams.RainInterval;
        end;
      end;

      PrevTime := Time;
      PrevQual := RainQual;
    end;

    if DispDelay(1) then Disp('Extracting rain events: '+StIdToS(HyFile.HSite)+' '+InSuffix+' '+VariableAsString(RainVarFrom)+' '+TToS(Time,True));
  end;

{ Compute totals and intensities for events }
  EventList.CalculateRainfallTotals(HyFile);

{ Remove events which do not exceed the total threshold }
  if RemoveEvents then begin
    for i := EventList.Count-1 downto 0 do begin
      with TEvent(EventList.Items[i]) do begin
        RainEventParams := TRainEventParams(RainEventParamsList.Find(HyFile.HSite,StartOfRain));
        if TotalRain < RainEventParams.MinEventTotal then EventList.Delete(i);
      end;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure MergeEvents(HyFile: THyFile; FlowEventList,RainEventList,StormEventList: TEventList);
var
  Event,Event1,Event2: TEvent;
  i,j: Integer;
begin
{ Put flow and rain events into a single list }
  for i := 0 to FlowEventList.Count-1 do begin
    Event := TEvent(FlowEventList.Items[i]);
    StormEventList.Add(TEvent.Create(Event));
  end;
  for i := 0 to RainEventList.Count-1 do begin
    Event := TEvent(RainEventList.Items[i]);
    StormEventList.Add(TEvent.Create(Event));
  end;

{ Merge flow and rain events to give storm events }
  for i := StormEventList.Count-1 downto 0 do begin
    Event1 := TEvent(StormEventList.Items[i]);
    for j := StormEventList.Count-1 downto 0 do begin
      Event2 := TEvent(StormEventList.Items[j]);
      if i <> j then begin
        if ((Event1.StartOfStorm >= Event2.StartOfStorm) and (Event1.StartOfStorm <= Event2.EndOfStorm)) or
           ((Event1.EndOfStorm >= Event2.StartOfStorm) and (Event1.EndOfStorm <= Event2.EndOfStorm)) then begin
          if Event1.StartOfStorm < Event2.StartOfStorm then Event2.StartOfStorm := Event1.StartOfStorm;
          if Event1.EndOfStorm > Event2.EndOfStorm then Event2.EndOfStorm := Event1.EndOfStorm;
          StormEventList.Delete(i); { delete Event1 }
          Break;
        end;
      end;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure ExtractCombinedEvents(HyFile: THyFile; SiteDetails: TSiteDetails;
  StormEventList,CombinedEventList: TEventList; AltEvents: Boolean);
var
  FlowEventList,RainEventList: TEventList;
  Event,{NextEvent,}ExtraEvent: TEvent;
  FlowEventParams: TFlowEventParams;
//RainEventParams: TRainEventParams;
//EndSearchTime: TRelTime;
  i,j: Integer;
begin
{ Calculate combined events }
  FlowEventList := TEventList.Create;
  RainEventList := TEventList.Create;
  try
    for i := 0 to StormEventList.Count-1 do begin
      FlowEventList.Clear;
      RainEventList.Clear;
      with TEvent(StormEventList.Items[i]) do begin
        if AltEvents
          then Event := TAltEvent.Create(TEvent(StormEventList.Items[i]))
          else Event := TEvent.Create(TEvent(StormEventList.Items[i]));

      { Compute time to stop searching }
        //EndSearchTime   := NullTime;
        FlowEventParams := nil;
        if RainMode = rmFlow then begin
          FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(evSite,StartOfFlow));
          //EndSearchTime   := TimeOffset (EndOfFlow, iMinute, +FlowEventParams.EndExtendInterval);
        end else if RainMode = rmRain then begin
          //RainEventParams := TRainEventParams(RainEventParamsList.Find(evSite,StartOfRain));
          //EndSearchTime   := TimeOffset (EndOfRain, iMinute, +RainEventParams.EndExtendInterval);
        end else if RainMode = rmBoth then begin
          FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(evSite,StartOfFlow));
          //RainEventParams := TRainEventParams(RainEventParamsList.Find(evSite,StartOfRain));
          //EndSearchTime   := Max(
          //  TimeOffset (EndOfFlow, iMinute, +FlowEventParams.EndExtendInterval),
          //  TimeOffset (EndOfRain, iMinute, +RainEventParams.EndExtendInterval));
        end;
        if (i < StormEventList.Count-1) and Assigned(FlowEventParams) then begin
          // NextEvent     := TEvent(StormEventList.Items[i+1]);
          //EndSearchTime := Min(
          //  EndSearchTime,
          //  TimeOffset (NextEvent.StartOfStorm, iMinute, -FlowEventParams.RiseInterval));
        end;

      { Extract flow and rain events for storm }
        if SiteDetails.HasFlowEvent then ExtractFlowEvents(HyFile,StartOfStorm,EndOfStorm,FlowEventList,False);
        if SiteDetails.HasRainEvent then ExtractRainEvents(HyFile,StartOfStorm,EndOfStorm,RainEventList,False);

      { Assign rain mode }
        if (RainEventList.Count > 0) and (FlowEventList.Count > 0) then begin
          Event.RainMode := rmBoth;
        end else if (RainEventList.Count > 0) then begin
          Event.RainMode := rmRain;
        end else if (FlowEventList.Count > 0) then begin
          Event.RainMode := rmFlow;
        end;

      { Assign start and end of flow }
        if FlowEventList.Count > 0 then begin
          Event.StartOfFlow := TEvent(FlowEventList.Items[0]).StartOfFlow;
          Event.EndOfFlow   := TEvent(FlowEventList.Items[FlowEventList.Count-1]).EndOfFlow;
          Event.FlowMode    := TEvent(FlowEventList.Items[0]).FlowMode;
        end;

      { Assign start and end of rain }
        if RainEventList.Count > 0 then begin
          Event.StartOfRain := TEvent(RainEventList.Items[0]).StartOfRain;
          Event.EndOfRain   := TEvent(RainEventList.Items[RainEventList.Count-1]).EndOfRain;
          Event.RainModeNum := RainEventList.Count;
        end;

      { If more than one rain event, output all rain events as "extra" events }
        if (RainEventList.Count > 1) and (FlowEventList.Count > 0) and (not AltEvents) then begin
          for j := 0 to RainEventList.Count-1 do begin
            ExtraEvent := TEvent.Create(TEvent(RainEventList.Items[j]));
            with ExtraEvent do begin
              RainMode := rmExtra;
              EventID  := Event.EventID;
            end;
            CombinedEventList.Add(ExtraEvent);
          end;
        end;

        CombinedEventList.Add(Event);
      end;
    end;
  finally
    FlowEventList.Free;
    RainEventList.Free;
  end;
end;

{------------------------------------------------------------------------------}

procedure ComputeSite(HyFile: THyFile; SiteDetails: TSiteDetails);
var
  FlowEventList,RainEventList,StormEventList: TEventList;
  TempStartTime,TempEndTime: TRelTime;
  AnyLevelData,AnyFlowData,AnyRainData: Boolean;
begin
  FlowEventList   := TEventList.Create;
  RainEventList   := TEventList.Create;
  StormEventList := TEventList.Create;
  try
  { Get combined period of record times }
    TempStartTime := HUGETIME;
    TempEndTime   := -HUGETIME;
    AnyLevelData  := CheckTimes(HyFile,LevelVarFrom,TempStartTime,TempEndTime);
    AnyFlowData   := CheckTimes(HyFile,FlowVarFrom,TempStartTime,TempEndTime);
    AnyRainData   := CheckTimes(HyFile,RainVarFrom,TempStartTime,TempEndTime);
    if PeriodOfRecord then begin
      StartTime := TempStartTime;
      EndTime   := TempEndTime;
    end;

  { Compute recession slope - input file must be closed as we may need to write
    the recession slope to the input file }
    if WriteSlope then begin
      try
        HyFile.Free;
        ComputeRecessionSlope(SiteDetails.sdSite,StartTime,EndTime);
      finally
        HyFile := THyFile.Create(SiteDetails.sdSite,InSuffix);
      end;
    end;

  { Extract flow events }
    if (AnyLevelData or AnyFlowData) and SiteDetails.HasFlowEvent then begin
      ExtractFlowEvents(HyFile,StartTime,EndTime,FlowEventList,True);
      FlowEventList.AssignEventIDs(False);
    end;

  { Extract rain events }
    if AnyRainData and SiteDetails.HasRainEvent then begin
      ExtractRainEvents(HyFile,StartTime,EndTime,RainEventList,True);
      RainEventList.AssignEventIDs(False);
    end;

  { Extend start and end of events }
    FlowEventList.ExtendEvents;
    RainEventList.ExtendEvents;

  { Merge flow events with rain events }
    StormEventList.Sorted := True;
    MergeEvents(HyFile,FlowEventList,RainEventList,StormEventList);

  { Calculate combined events }
    ExtractCombinedEvents(HyFile,SiteDetails,StormEventList,CombinedEventList,False);
    CombinedEventList.CalculateRainfallTotals(HyFile);
    CombinedEventList.CalculateFlowTotals(HyFile);
    CombinedEventList.Sorted := True;
    CombinedEventList.AssignEventIDs(False);
    CombinedEventList.CalculateStatistics(HyFile,SiteDetails,False);

  { Print event summaries }
    if HyDebug then begin
      FlowEventList.WriteToList('Provisional flow events:',False);
      RainEventList.WriteToList('Provisional rainfall events:',False);
      StormEventList.WriteStormsToList('Storm events:');
    end;
    CombinedEventList.WriteToList(iifStr(HyDebug,'Combined events:',''),False);

  finally
    FlowEventList.Free;
    RainEventList.Free;
    StormEventList.Free;
  end;
end;

{------------------------------------------------------------------------------}

procedure UpdateSite(var HyFile: THyFile; SiteDetails: TSiteDetails);
var
  TempStartTime,TempEndTime: TRelTime;
begin
{ Load stored events and recompute }
  CombinedEventList.LoadRecords(EventDB,TableNames.Values['Event Output'],SiteDetails.sdSite);
  CombinedEventList.CalculateRainfallTotals(HyFile);
  CombinedEventList.CalculateFlowTotals(HyFile);
  CombinedEventList.Sorted := True;
  CombinedEventList.AssignEventIDs(True);
  CombinedEventList.CalculateStatistics(HyFile,SiteDetails,False);

{ Compute recession slope - input file must be closed as we may need to write
  the recession slope to the input file }
  if WriteSlope then begin
    TempStartTime := HUGETIME;
    TempEndTime   := -HUGETIME;
    CheckTimes(HyFile,FlowVarFrom,TempStartTime,TempEndTime);
    try
      HyFile.Free;
      ComputeRecessionSlope(SiteDetails.sdSite,TempStartTime,TempEndTime);
    finally
      HyFile := THyFile.Create(SiteDetails.sdSite,InSuffix);
    end;
  end;

{ Print updated event summary }
  CombinedEventList.WriteToList('',False);
end;

{------------------------------------------------------------------------------}

procedure ValidateSite(HyFile: THyFile; SiteDetails: TSiteDetails);
var
  StormEventList: TEventList;
begin
  StormEventList := TEventList.Create;
  try
  { Load stored events and re-extend }
    StormEventList.Sorted := True;
    StormEventList.LoadRecords(EventDB,TableNames.Values['Event Output'],SiteDetails.sdSite);
    StormEventList.ExtendEvents;

  { Calculate combined events }
    ExtractCombinedEvents(HyFile,SiteDetails,StormEventList,CombinedEventList,True);
    CombinedEventList.CalculateRainfallTotals(HyFile);
    CombinedEventList.CalculateFlowTotals(HyFile);
    CombinedEventList.Sorted := True;
    CombinedEventList.AssignEventIDs(True);
    CombinedEventList.CalculateStatistics(HyFile,SiteDetails,False);

  { Print event summaries }
    if HyDebug then begin
      StormEventList.WriteStormsToList('Storm events:');
    end;
    CombinedEventList.WriteToList(iifStr(HyDebug,'Combined events:',''),False);

  finally
    StormEventList.Free;
  end;
end;

{------------------------------------------------------------------------------}

procedure StatisticsSite(HyFile: THyFile; SiteDetails: TSiteDetails);
begin
{ Load stored events }
  CombinedEventList.Sorted := True;
  CombinedEventList.LoadRecords(EventDB,TableNames.Values['Event Output'],SiteDetails.sdSite);
  CombinedEventList.CalculateStatistics(HyFile,SiteDetails,True);

{ Print event summary with statistics }
  CombinedEventList.WriteToList('',True);
end;

{------------------------------------------------------------------------------}

procedure EMCSite(HyFile: THyFile; SiteDetails: TSiteDetails);
var
  Site: TStID;
  EventList: TEventList;
  WQSampleList,TSSampleList: TPointList;
  TempValue: TReal;
  TempQuality,i: Integer;
  WQHyFile: THyFile;
begin
  Site := SiteDetails.sdSite;
  if not DataSourceOK(Site,OutSuffix) then begin
    EventList    := TEventList.Create;
    WQSampleList := TPointList.Create;
    TSSampleList := TPointList.Create;
    try
    { Write sample times to output file }
      with WQSampleList do begin
        LoadWQPoints(WQDB,Site,StartTime,EndTime,stStormWater);
        WriteToFile(Site,OutSuffix,350.00);
        LoadWQPoints(WQDB,Site,StartTime,EndTime,stExcluded);
        WriteToFile(Site,OutSuffix,350.01);
        LoadWQPoints(WQDB,Site,StartTime,EndTime,stBaseFlow);
        WriteToFile(Site,OutSuffix,350.02);
        LoadWQPoints(WQDB,Site,StartTime,EndTime,stPrimary);
        WriteToFile(Site,OutSuffix,350.10);
        LoadWQPoints(WQDB,Site,StartTime,EndTime,stSecondary);
        WriteToFile(Site,OutSuffix,350.11);
      end;

    { Load records }
      Disp('Loading events...');
      CombinedEventList.LoadRecords(EventDB,TableNames.Values['Event Output'],Site);
      EventList.LoadAllRecords(EventDB,TableNames.Values['Event Output'],Site);
      EventList.CalculateFlowTotals(HyFile);
      Disp('Loading WQ samples...');
      WQSampleList.LoadWQPoints(WQDB,Site,StartTime,EndTime,stEMC);
      Disp('Loading TS points...');
      TSSampleList.LoadTSPoints(HyFile,FlowVarFrom,StartTime,EndTime);

    { Insert event start and end times into WQ samples }
      for i := 0 to CombinedEventList.Count-1 do with TEvent(CombinedEventList.Items[i]) do begin
        if EMCValid and (FlowQual <= ConfigRec.MaxGoodQual) then begin
          WQSampleList.AddEvent(TEvent(CombinedEventList.Items[i]));
        end;
        if DispDelay(1) then Disp('Inserting event start and end times: '+StIdToS(Site)+' '+EventID);
      end;

    { Insert WQ sample times into level trace }
      for i := 0 to WQSampleList.Count-1 do with TPoint(WQSampleList.Items[i]) do begin
        if Quality <= ConfigRec.MaxGoodQual then begin
          TSSampleList.AddPoint(TPoint(WQSampleList.Items[i]));
        end;
        if DispDelay(1) then Disp('Inserting WQ sample times: '+StIdToS(Site)+' '+VariableAsString(Variable)+' '+TToS(Time,True));
      end;

    { Convert levels to discharge }
      for i := 0 to TSSampleList.Count-1 do with TPoint(TSSampleList.Items[i]) do begin
        TempValue   := Value;
        TempQuality := Quality;
        HyFile.VarConList.VarConvert(FlowVarFrom,FlowVarTo,TempValue,TempValue,Time,0,TempQuality,1);
        HyFile.VarConList.VarConvert(FlowVarFrom,FlowVarTo,TempValue,TempValue,Time,0,TempQuality,2);
        Variable    := FlowVarTo;
        Value       := TempValue;
        Quality     := TempQuality;
      end;

    { Write WQ samples and flow data to output file }
      WQSampleList.WriteToFile(Site,OutSuffix,NullVariable);
      TSSampleList.WriteToFile(Site,OutSuffix,NullVariable);

    { Compute EMCs }
      WQHyFile := THyFile.Create(Site,OutSuffix);
      try
        CombinedEventList.CalculateEMCs(WQHyFile,EventList.MedianVolume);
      finally
        WQHyFile.Free;
      end;
    finally
      EventList.Free;
      WQSampleList.Free;
      TSSampleList.Free;
    end;
  end else begin
    Abort('output file already exists');
  end;
end;

{------------------------------------------------------------------------------}

procedure DoMain;
var
  SiteList: TSiteList;
  lSite: TStID;
  SiteDetails: TSiteDetails;
  HyFile: THyFile;
  i,j: Integer;
begin
  LoadParams;
  LoadIniFile;
  ListDev := tListDevice.create (ListDevStr);

{ Create global variables }
  SiteDetailsList     := TSiteDetailsList.Create;
  FlowEventParamsList := TFlowEventParamsList.Create;
  RainEventParamsList := TRainEventParamsList.Create;
  CombinedEventList   := TEventList.Create;
  EventVariableList   := TEventVariableList.Create;
  EMCVariableList     := TEMCVariableList.Create;
  BaseFlowParamsList  := TBaseFlowParamsList.Create;
  SiteList            := TSiteList.Create(SiteExpr);

  try
  { Load configuration tables }
    SiteDetailsList.LoadRecords(EventDB,TableNames.Values['Sites']);
    FlowEventParamsList.LoadRecords(EventDB,TableNames.Values['Flow Event Parameters']);
    RainEventParamsList.LoadRecords(EventDB,TableNames.Values['Rain Event Parameters']);
    EventVariableList.LoadRecords(EventDB,TableNames.Values['Event Variables']);
    if ProgMode = pmEMC then begin
      EMCVariableList.LoadRecords(WQDB,TableNames.Values['EMC Variables']);
      BaseFlowParamsList.LoadRecords(WQDB,TableNames.Values['Baseflow Parameters']);
      if WriteTable then begin
        CreateTable(EventDB,TableNames.Values['EMC Output'],TEMC.Create(nil,nil));
        CreateTable(EventDB,TableNames.Values['Load Output'],TEMCPartition.Create(nil,0,'',0,NullVariable,NullVariable,0,0));
      end;
    end;

  { Write report header }
    ListDev.Writeln(ConfigRec.LetterHead+StrPadLeft(Signature,170-Length(ConfigRec.LetterHead),' '));
    ListDev.Writeln;
    if ProgMode = pmEMC then begin
      ListDev.Writeln('Site            Event ID  Variable Samples ---------- Volume Scores --------- ----------- Load Scores ----------- -- Other Scores -    Final');
      ListDev.Writeln('                                              Head Interior     Tail    Total     Head Interior     Tail    Total   Sample FlowRate    Score');
      ListDev.Writeln('--------------------------------------------------------------------------------------------------------------------------------------------');
    end else begin
      ListDev.Writeln('Site            Suffix Event ID  Rain Mode Start Time       End Time         Total Rain  Intensity  RQ Flow Mode Start Time       EndTime          Total Flow  FQ         RV RVQ');
      ListDev.Writeln('--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------');
    end;

  { Process each site }
    while SiteList.Another(lSite) do begin
      if DataSourceOK(lSite,InSuffix) then begin
        HyFile := THyFile.Create(lSite,InSuffix);
        try
          CombinedEventList.Clear;
          CombinedEventList.Sorted := False;

          SiteDetails := SiteDetailsList.FindSite(lSite);
          if Assigned(SiteDetails) then begin

          { Override parameter screen variables with site-specific variable }
            if not VariablesEQ(SiteDetails.FlowVar,NullVariable) then begin
              LevelVarFrom  := SiteDetails.FlowVar;
              FlowVarFrom   := SiteDetails.FlowVar;
              VolumeVarFrom := SiteDetails.FlowVar;
            end;
            if ProgMode = pmCompute then begin
              ComputeSite(HyFile,SiteDetails);
              if WriteTable then begin
                CombinedEventList.WriteToTable(EventDB,TableNames.Values['Event Output'],wmInsert);
              end;
            end else if ProgMode = pmUpdate then begin
              UpdateSite(HyFile,SiteDetails);
              if WriteTable then begin
                CombinedEventList.WriteToTable(EventDB,TableNames.Values['Event Output'],wmUpdate);
              end;
            end else if ProgMode = pmValidate then begin
              ValidateSite(HyFile,SiteDetails);
              if WriteTable then begin
                CombinedEventList.WriteToTable(EventDB,TableNames.Values['Event Output'],wmUpdate);
              end;
            end else if ProgMode = pmStatistics then begin
              StatisticsSite(HyFile,SiteDetails);
              if WriteTable then begin
                for i := 0 to CombinedEventList.Count-1 do with TEvent(CombinedEventList.Items[i]) do begin
                  Statistics.WriteToTable(EventDB,TableNames.Values['Statistics Output'],wmBoth);
                end;
              end;
            end else if ProgMode = pmEMC then begin
              ApplyUpdates(EventDB);
              EMCSite(HyFile,SiteDetails);
              if WriteTable then begin
                Disp('Writing EMC partitions and results...');
                DeleteTable(EventDB,TableNames.Values['EMC Output'],lSite);
                DeleteTable(EventDB,TableNames.Values['Load Output'],lSite);
                for i := 0 to CombinedEventList.Count-1 do with TEvent(CombinedEventList.Items[i]) do begin
                  EMCList.WriteToTable(EventDB,TableNames.Values['EMC Output'],wmAdd);
                  for j := 0 to EMCList.Count-1 do with TEMC(EMCList.Items[j]) do begin
                    PartitionList.WriteToTable(EventDB,TableNames.Values['Load Output'],wmAdd);
                  end;
                end;
              end;
            end;
          end else begin
            Abort('Unable to find site '+StIdToS(lSite)+' in table '+TableNames.Values['Sites']);
          end;
        finally
          HyFile.Free;
        end;
        if WriteEvent then CombinedEventList.WriteToFile(lSite,OutSuffix,EventVar);
      end else begin
        DispPush([dspWarning]);
        try
          Disp('Unable to open '+StIdToS(lSite)+'.'+InSuffix);
        finally
          DispPop;
        end;
      end;
    end;
    ListDev.Close;
  finally
    ListDev.free;
    SiteDetailsList.Free;
    FlowEventParamsList.Free;
    RainEventParamsList.Free;
    CombinedEventList.Free;
    EventVariableList.Free;
    EMCVariableList.Free;
    BaseFlowParamsList.Free;
    SiteList.Free;
    TableNames.Free; { Created in LoadIniFile }
    EventDB.Free;    { Created in LoadIniFile }
    if ProgMode = pmEMC then begin
      WQDB.Free;     { Created in LoadIniFile }
    end;
  end;
end;

{------------------------------------------------------------------------------}
{                        TMySortedValueList object                             }
{------------------------------------------------------------------------------}

function TMySortedValueList.GetMedian: TReal;
begin
  if Count > 0 then begin
  { If list has an even number of values, the median is the average of the middle two values }
    if Count mod 2 = 0 then begin
      Result := (TValueObject(Items[Trunc(Count/2)-1]).Val+
                 TValueObject(Items[Trunc(Count/2)]).Val)/2;

  { If list has an odd number of values, the median is the middle value }
    end else begin
      Result := TValueObject(Items[Trunc((Count)/2)]).Val;
    end;
  end else begin
    Result := 0;
  end;
end;

{------------------------------------------------------------------------------}
{                             TADODB object                               }
{------------------------------------------------------------------------------}

constructor TADODB.Create(ConnectStr: String);
begin
  inherited Create;

{ Open database }
  fADOConnection := TADOConnection.Create(nil);
  with fADOConnection do begin
    ConnectionString := ConnectStr;
    LoginPrompt := False;
    Open;
  end;

{ Create query }
  fADOQuery := TADOQuery.Create(nil);
  with fADOQuery do begin
    Connection := fADOConnection;
    SQL.Clear;
  end;
end;

destructor TADODB.Destroy;
begin
  fADOConnection.Free;
  fADOQuery.Free;
  inherited Destroy;
end;

procedure TADODB.AddSQL(SQLStatement: String);
begin
  if HyDebug then DispDebug('SQL',SQLStatement);
  with fADOQuery do begin
    SQL.Clear;
    SQL.Add(SQLStatement);
  end;
end;

{------------------------------------------------------------------------------}
{                             TMyAutoList object                               }
{------------------------------------------------------------------------------}

constructor TMyAutoList.Create;
begin
  inherited Create(10,False,True);
end;

{------------------------------------------------------------------------------}

procedure TMyAutoList.WriteToTable(DB: TADODB; TableName: String; WriteMode: TWriteMode);

  function FindRecord(TableName,Where: String): Boolean;
  begin
    with DB.ADOQuery do begin
      DB.AddSQL('SELECT * FROM '+TableName+' WHERE ('+Where+')');
      Open;
      Result := not(BOF and EOF);
      Close;
    end;
  end;

var
  i: Integer;
begin
  with DB do begin
    for i := 0 to Count-1 do with TTableRecord(Items[i]) do begin

    { Insert new record, abort if already there }
      if WriteMode = wmInsert then begin
        if not FindRecord(TableName,Where) or (TEvent(Items[i]).RainMode = rmExtra) { "extra" events may have duplicate event IDs }
          then AddSQL('INSERT INTO '+TableName+' ('+FieldNames+') VALUES ('+FieldValues+')')
          else Abort('Record already exists in '+TableName+' table for ('+Where+')');

    { Update existing record, abort if not found }
      end else if WriteMode = wmUpdate then begin
        if FindRecord(TableName,Where)
          then AddSQL('UPDATE '+TableName+' SET '+SetList+' WHERE ('+Where+')')
          else Abort('Unable to find record ('+Where+') in '+TableName+' table');

    { Insert new record, update if already there }
      end else if WriteMode = wmBoth then begin
        if not FindRecord(TableName,Where)
          then AddSQL('INSERT INTO '+TableName+' ('+FieldNames+') VALUES ('+FieldValues+')')
          else AddSQL('UPDATE '+TableName+' SET '+SetList+' WHERE ('+Where+')');

    { Insert new record without checking for existing record }
      end else if WriteMode = wmAdd then begin
        AddSQL('INSERT INTO '+TableName+' ('+FieldNames+') VALUES ('+FieldValues+')');
      end;

      ADOQuery.ExecSQL;
    end;
    ADOQuery.Close;
  end;
end;

{------------------------------------------------------------------------------}
{                           TTableRecord object                                }
{------------------------------------------------------------------------------}

constructor TTableRecord.Create;
begin
  inherited Create;
  fFieldNames  := TStringList.Create;
  fFieldValues := TStringList.Create;
  fFieldTypes  := TStringList.Create;
  fWhere       := TStringList.Create;
  fSetList     := TStringList.Create;
end;

destructor TTableRecord.Destroy;
begin
  fFieldNames.Free;
  fFieldValues.Free;
  fFieldTypes.Free;
  fWhere.Free;
  fSetList.Free;
  inherited Destroy;
end;

function TTableRecord.GetFieldNames: String;
begin
{ Comma delimited list of field names that are written to in COMPUTE mode }
  Result := StrListToStr(fFieldNames,',');
end;

function TTableRecord.GetFieldValues: String;
begin
{ Comma delimited list of field values that are written to in COMPUTE mode }
  Result := StrListToStr(fFieldValues,',');
end;

function TTableRecord.GetFieldTypes: String;
begin
{ Comma delimited list of field names and types that are written in CREATE TABLE }
  Result := StrListToStr(fFieldTypes,',');
end;

function TTableRecord.GetWhere: String;
begin
{ Key fields and values to find record }
  Result := StrListToStr(fWhere,' AND ');
end;

function TTableRecord.GetSetList: String;
begin
{ Fields and values that are written to in UPDATE mode }
  Result := StrListToStr(fSetList,',');
end;

function TTableRecord.GetDisplayText: String;
begin
  Result := '';
end;

function TTableRecord.Quote(Str: String): String;
begin
  Result := ''''+Str+'''';
end;

{------------------------------------------------------------------------------}
{                             TEventParams objects                             }
{------------------------------------------------------------------------------}

constructor TEventParams.Create(aSite: TStID; StartDate: TRelTime;
  SitePeriod,StartExtendPeriod,EndExtendPeriod: Integer);
begin
  inherited Create;
  fSite              := aSite;
  fStartDate         := StartDate;
  fSitePeriod        := SitePeriod;
  fStartExtendPeriod := StartExtendPeriod;
  fEndExtendPeriod   := EndExtendPeriod;
end;

function TEventParams.GetSiteInterval: Integer;
begin
  Result := Round(fSitePeriod / 60); { minutes }
end;

function TEventParams.GetStartExtendInterval: TReal;
begin
  Result := (fSitePeriod * fStartExtendPeriod) / 60; { minutes }
end;

function TEventParams.GetEndExtendInterval: TReal;
begin
  Result := (fSitePeriod * fEndExtendPeriod) / 60; { minutes }
end;

{------------------------------------------------------------------------------}
{                             TFlowEventParams objects                         }
{------------------------------------------------------------------------------}

constructor TFlowEventParams.Create(aSite: TStID; StartDate: TRelTime;
  SitePeriod,StartExtendPeriod,EndExtendPeriod: Integer;
  Method: TFlowMode;
  TimeThreshold,LevelThreshold,FlowThreshold,VolumeThreshold,RecessionThreshold: TReal;
  RisePeriod,DropPeriod,DelayPeriod: Integer);
begin
  inherited Create(aSite,StartDate,SitePeriod,StartExtendPeriod,EndExtendPeriod);
  fMethod             := Method;
  fTimeThreshold      := TimeThreshold;
  fLevelThreshold     := LevelThreshold;
  fFlowThreshold      := FlowThreshold;
  fVolumeThreshold    := VolumeThreshold;
  fRecessionThreshold := RecessionThreshold;
  fRisePeriod         := RisePeriod;
  fDropPeriod         := DropPeriod;
  fDelayPeriod        := DelayPeriod;
end;

function TFlowEventParams.GetRiseInterval: TReal;
begin
  Result := (fSitePeriod * fRisePeriod) / 60; { minutes }
end;

function TFlowEventParams.GetDropInterval: TReal;
begin
  Result := (fSitePeriod * fDropPeriod) / 60; { minutes }
end;

function TFlowEventParams.GetDelayInterval: TReal;
begin
  Result := (fSitePeriod * fDelayPeriod) / 60; { minutes }
end;

{------------------------------------------------------------------------------}
{                             TRainEventParams objects                         }
{------------------------------------------------------------------------------}

constructor TRainEventParams.Create(aSite: TStID; StartDate: TRelTime; SitePeriod,ExtendPeriod: Integer;
  IntervalPeriod: Integer; MinIntervalTotal,MinEventTotal: TReal);
begin
  inherited Create(aSite,StartDate,SitePeriod,ExtendPeriod,ExtendPeriod);
  fIntervalPeriod   := IntervalPeriod;
  fMinIntervalTotal := MinIntervalTotal;
  fMinEventTotal    := MinEventTotal;
end;

function TRainEventParams.GetRainInterval: TReal;
begin
  Result := (fSitePeriod * fIntervalPeriod) / 60; { minutes }
end;

function TRainEventParams.GetDrizzleRate: TReal;
begin
  Result := (fMinIntervalTotal / RainInterval) * 60; { per hour }
end;

{------------------------------------------------------------------------------}
{                            TEventParamsList object                           }
{------------------------------------------------------------------------------}

function TEventParamsList.Find(aSite: TStID; Time: TRelTime): TEventParams;
var
  EventParams: TEventParams;
  i: Integer;
begin
  Result := nil;
  for i := Count-1 downto 0 do begin
    EventParams := TEventParams(Items[i]);
    if StIDCompare(aSite,EventParams.epSite) = 0 then begin
      if Time >= EventParams.StartDate then begin
        Result := EventParams;
        Break;
      end;
    end;
  end;
  if not Assigned(Result) then Abort('Cannot find event parameters for site '+StIdToS(aSite)+' at time '+TToS(Time,False));
end;

{------------------------------------------------------------------------------}
{                          TFlowEventParamsList object                         }
{------------------------------------------------------------------------------}

procedure TFlowEventParamsList.LoadRecords(DB: TADODB; TableName: String);
begin
  with DB.ADOQuery do begin
    DB.AddSQL('SELECT * FROM '+TableName);
    Open;
    First;
    while not EOF do begin
      Add(TFlowEventParams.Create(
        SToStID(FieldByName('Site').AsString),
        DateTimeToRel(StrToDateTime(FieldByName('Start_Date').AsString),0,True),
        FieldByName('Site_Period').AsInteger,
        FieldByName('Start_Flow_Extend_Periods').AsInteger,
        FieldByName('End_Flow_Extend_Periods').AsInteger,
        StrToFlowMode(FieldByName('Method').AsString),
        FieldByName('Time_Threshold').AsFloat,
        FieldByName('Level_Threshold').AsFloat,
        FieldByName('Flow_Threshold').AsFloat,
        FieldByName('Volume_Threshold').AsFloat,
        FieldByName('Daily_Recession').AsFloat,
        FieldByName('Rise_Periods').AsInteger,
        FieldByName('Drop_Periods').AsInteger,
        FieldByName('Delay_Periods').AsInteger));
      Next;
    end;
    Close;
  end;
end;

{------------------------------------------------------------------------------}
{                          TRainEventParamsList object                         }
{------------------------------------------------------------------------------}

procedure TRainEventParamsList.LoadRecords(DB: TADODB; TableName: String);
begin
  with DB.ADOQuery do begin
    DB.AddSQL('SELECT * FROM '+TableName);
    Open;
    First;
    while not EOF do begin
      Add(TRainEventParams.Create(
        SToStID(FieldByName('Site').AsString),
        DateTimeToRel(StrToDateTime(FieldByName('Start_Date').AsString),0,True),
        FieldByName('Site_Period').AsInteger,
        FieldByName('Rain_Extend_Periods').AsInteger,
        FieldByName('Rain_Interval_Periods').AsInteger,
        FieldByName('Min_Interval_Total').AsFloat,
        FieldByName('Min_Event_Total').AsFloat));
      Next;
    end;
    Close;
  end;
end;

{------------------------------------------------------------------------------}
{                               TEvent object                                  }
{------------------------------------------------------------------------------}

constructor TEvent.Create(aSite: TStID; Suffix: string; ID: Integer);
begin
  inherited Create;
  fSite        := aSite;
  fSuffix      := Suffix;
  fID          := ID;
  fEventID     := '';
  fNewEventID  := '';
  fRainMode    := rmNone;
  fRainModeNum := 0;
  fStartOfRain := 0;
  fEndOfRain   := 0;
  fTotalRain   := 0;
  fIntensity   := 0;
  fRainQual    := ConfigRec.DefGoodQual;
  fFlowMode    := fmNone;
  fStartOfFlow := 0;
  fEndOfFlow   := 0;
  fTotalFlow   := 0;
  fFlowQual    := ConfigRec.DefGoodQual;
  fFlowValid   := False;
  fEMCValid    := False;
  fStartOfStorm:= 0;
  fEndOfStorm  := 0;
  fStatistics  := TEventStatisticList.Create(fID);
  fEMCList     := TEMCList.Create;
end;

{------------------------------------------------------------------------------}

constructor TEvent.Create(Event: TEvent);
begin
  inherited Create;
  fSite        := Event.fSite;
  fSuffix      := Event.fSuffix;
  fID          := Event.fID;
  fEventID     := Event.fEventID;
  fNewEventID  := Event.fNewEventID;
  fRainMode    := Event.fRainMode;
  fRainModeNum := Event.fRainModeNum;
  fStartOfRain := Event.fStartOfRain;
  fEndOfRain   := Event.fEndOfRain;
  fTotalRain   := Event.fTotalRain;
  fIntensity   := Event.fIntensity;
  fRainQual    := Event.fRainQual;
  fFlowMode    := Event.fFlowMode;
  fStartOfFlow := Event.fStartOfFlow;
  fEndOfFlow   := Event.fEndOfFlow;
  fTotalFlow   := Event.fTotalFlow;
  fFlowQual    := Event.fFlowQual;
  fFlowValid   := Event.fFlowValid;
  fEMCValid    := Event.fEMCValid;
  fStartOfStorm:= Event.fStartOfStorm;
  fEndOfStorm  := Event.fEndOfStorm;
  fStatistics  := TEventStatisticList.Create(Event.fID);
  fEMCList     := TEMCList.Create;
end;

{------------------------------------------------------------------------------}

destructor TEvent.Destroy;
begin
  fStatistics.Free;
  fEMCList.Free;
  inherited Destroy;
end;

{------------------------------------------------------------------------------}

function TEvent.GetStartOfEvent: TRelTime;
begin
  case fRainMode of
    rmNone:  Result := fStartOfFlow;
    rmRain:  Result := fStartOfRain;
    rmFlow:  Result := fStartOfFlow;
    rmBoth:  Result := Min(fStartOfRain,fStartOfFlow);
    rmExtra: Result := fStartOfRain;
  else
    Result := 0;
  end;
end;

{------------------------------------------------------------------------------}

function TEvent.GetEndOfEvent: TRelTime;
begin
  case fRainMode of
    rmNone:  Result := fEndOfFlow;
    rmRain:  Result := fEndOfRain;
    rmFlow:  Result := fEndOfFlow;
    rmBoth:  Result := Max(fEndOfRain,fEndOfFlow);
    rmExtra: Result := fEndOfRain;
  else
    Result := 0;
  end;
end;

{------------------------------------------------------------------------------}

function TEvent.GetRV: TEventStatistic;
var
  SiteDetails: TSiteDetails;
begin
  SiteDetails := SiteDetailsList.FindSite(fSite);
  if Assigned(SiteDetails) and SiteDetails.HasRV then begin
    Result := fStatistics.Find('RV');
  end else begin
    Result := nil;
  end;
end;

{------------------------------------------------------------------------------}

function TEvent.GetKey: String;
begin
  Result := StrPadRight (StIdToS(fSite), SiteIdLen, ' ')+fEventID;
  if fRainMode = rmExtra then begin
    Result := Result+RelToTemplateTimeStr(fStartOfRain,'YYYYMMDDHHIIEE');
  end;
end;

{------------------------------------------------------------------------------}

function TEvent.GetFieldNames: String;
begin
  with fFieldNames do begin
    Clear;
    Add('Site');
    Add('Suffix');
    Add('EventID');
    Add('RainMode');
    if not TimesEQ (fStartOfRain,0) then Add('StartOfRain');
    if not TimesEQ (fEndOfRain,0)   then Add('EndOfRain');
    Add('TotalRain');
    Add('Intensity');
    Add('RainQual');
    Add('FlowMode');
    if not TimesEQ (fStartOfFlow,0) then Add('StartOfFlow');
    if not TimesEQ (fEndOfFlow,0)   then Add('EndOfFlow');
    Add('FlowVolume');
    Add('FlowQual');
    if Assigned(RV) then begin
      Add('RV');
      Add('RVQual');
    end;
    Add('RainValid');
    Add('FlowValid');
    Add('HyComment');
  end;
  Result := inherited GetFieldNames;
end;

{------------------------------------------------------------------------------}

function TEvent.GetFieldValues: String;
begin
  with fFieldValues do begin
    Clear;
    Add(Quote(StIDToS(fSite)));
    Add(Quote(fSuffix));
    Add(Quote(fEventID));
    Add(Quote(RainModeToStr(fRainMode,fRainModeNum)));
    if not TimesEQ (fStartOfRain, NullTime) then Add(RelToTemplateTimeStr(fStartOfRain,DateFormat));
    if not TimesEQ (fEndOfRain,   NullTime) then Add(RelToTemplateTimeStr(fEndOfRain,DateFormat));
    Add(RToS(fTotalRain,10,3,fsFixed));
    Add(RToS(fIntensity,10,3,fsFixed));
    Add(IntToStr(fRainQual));
    Add(Quote(FlowModeToStr(fFlowMode)));
    if not TimesEQ (fStartOfFlow, NullTime) then Add(RelToTemplateTimeStr(fStartOfFlow,DateFormat));
    if not TimesEQ (fEndOfFlow,   NullTime) then Add(RelToTemplateTimeStr(fEndOfFlow,DateFormat));
    Add(RToS(fTotalFlow,10,4,fsFixed));
    Add(IntToStr(fFlowQual));
    if Assigned(RV) then begin
      Add(iifStr(RV.Quality <= ConfigRec.MaxGoodQual,RToS(RV.Value,10,3,fsFixed),'NULL'));
      Add(IntToStr(RV.Quality));
    end;
    Add(BToSTF(fRainQual <= ConfigRec.MaxGoodQual));
    Add(BToSTF(fFlowQual <= ConfigRec.MaxGoodQual));
    Add(Quote(ProgModeToStr(ProgMode)+' '+TToS(Today,False)+' '+ConfigRec.UserAccess.UserID));
  end;
  Result := inherited GetFieldValues;
end;

{------------------------------------------------------------------------------}

function TEvent.GetWhere: String;
begin
  with fWhere do begin
    Clear;
    Add('Site = '+Quote(StIDToS(fSite)));
    Add('Suffix = '+Quote(fSuffix));
    Add('EventID = '+Quote(fEventID));
    Add('RainMode <> '+Quote('Extra')); { "extra" events may have duplicate event IDs }
  end;
  Result := inherited GetWhere;
end;

{------------------------------------------------------------------------------}

function TEvent.GetSetList: String;
begin
  with fSetList do begin
    Clear;
    if UpdateEventID then Add('EventID = '+Quote(fNewEventID)); { Write new event ID in UPDATE mode }
    Add('TotalRain = '+RToS(fTotalRain,10,3,fsFixed));
    Add('Intensity = '+RToS(fIntensity,10,3,fsFixed));
    Add('RainQual = '+IntToStr(fRainQual));
    Add('FlowVolume = '+RToS(fTotalFlow,10,4,fsFixed));
    Add('FlowQual = '+IntToStr(fFlowQual));
    if Assigned(RV) then begin
      Add('RV = '+iifStr(RV.Quality <= ConfigRec.MaxGoodQual,RToS(RV.Value,10,3,fsFixed),'NULL'));
      Add('RVQual = '+IntToStr(RV.Quality));
    end;
    Add('HyComment = '+Quote(ProgModeToStr(ProgMode)+' '+TToS(Today,False)+' '+ConfigRec.UserAccess.UserID));
  end;
  Result := inherited GetSetList;
end;

{------------------------------------------------------------------------------}

function TEvent.GetDisplayText: String;
begin
  Result := StrPadRight (StIdToS(fSite),SiteIdLen,' ')+' '+
            StrPadRight(fSuffix,6,' ')+' '+
            StrPadRight(iifStr(ProgMode in [pmCompute,pmStatistics],fEventID,fNewEventID),8,' ')+' '+
            StrPadRight(iifStr(fRainMode <> rmNone,RainModeToStr(fRainMode,fRainModeNum),''),9,' ')+' '+
            iifStr(fStartOfRain <> 0,TToS(fStartOfRain,False),StrReplicate(' ',16))+' '+
            iifStr(fEndOfRain <> 0,TToS(fEndOfRain,False),StrReplicate(' ',16))+' '+
            RToS(fTotalRain,10,3,fsFixed)+' '+
            RToS(fIntensity,10,3,fsFixed)+' '+
            IToS(fRainQual,3)+' '+
            StrPadRight(iifStr(fFlowMode <> fmNone,FlowModeToStr(fFlowMode),''),9,' ')+' '+
            iifStr(fStartOfFlow <> 0,TToS(fStartOfFlow,False),StrReplicate(' ',16))+' '+
            iifStr(fEndOfFlow <> 0,TToS(fEndOfFlow,False),StrReplicate(' ',16))+' '+
            RToS(fTotalFlow,10,4,fsFixed)+' '+
            IToS(fFlowQual,3);
  if Assigned(RV) then begin
    Result := Result+' '+
              iifStr(RV.Quality <= ConfigRec.MaxGoodQual,RToS(RV.Value,10,3,fsFixed),StrReplicate(' ',10))+' '+
              IToS(RV.Quality,3);
  end;
end;

{------------------------------------------------------------------------------}
{                               TAltEvent object                               }
{------------------------------------------------------------------------------}

function TAltEvent.GetWhere: String;
begin
{ Do not search Suffix field as validate mode suffix may be different to original compute mode suffix }
  with fWhere do begin
    Clear;
    Add('Site = '+Quote(StIDToS(fSite)));
    Add('EventID = '+Quote(fEventID));
    Add('RainMode <> '+Quote('Extra')); { "extra" events may have duplicate event IDs }
  end;
  Result := inherited GetWhere;
end;

function TAltEvent.GetSetList: String;
begin
  with fSetList do begin
    Clear;
    Add('AltRainStart = ' +iifStr(not TimesEQ (fStartOfRain,NullTime),RelToTemplateTimeStr(fStartOfRain,DateFormat),'NULL'));
    Add('AltRainEnd = '   +iifStr(not TimesEQ (fEndOfRain,  NullTime),RelToTemplateTimeStr(fEndOfRain,DateFormat),'NULL'));
    Add('AltTotalRain = ' +RToS(fTotalRain,10,3,fsFixed));
    Add('AltRainQual = '  +IntToStr(fRainQual));
    Add('AltFlowStart = ' +iifStr(not TimesEQ (fStartOfFlow,NullTime),RelToTemplateTimeStr(fStartOfFlow,DateFormat),'NULL'));
    Add('AltFlowEnd = '   +iifStr(not TimesEQ (fEndOfFlow,  NullTime),RelToTemplateTimeStr(fEndOfFlow,DateFormat),'NULL'));
    Add('AltFlowVolume = '+RToS(fTotalFlow,10,4,fsFixed));
    Add('AltFlowQual = '  +IntToStr(fFlowQual));
    Add('HyComment = '    +Quote(ProgModeToStr(ProgMode)+' '+TToS(Today,False)+' '+ConfigRec.UserAccess.UserID));
  end;
  //Result := inherited GetSetList;
  Result := StrListToStr(fSetList,',');
end;

{------------------------------------------------------------------------------}
{                               TEventList object                              }
{------------------------------------------------------------------------------}

function TEventList.GetMedianVolume: TReal;
var
  SortList: TMySortedValueList;
  i: Integer;
begin
  SortList := TMySortedValueList.Create(100,True);
  try
    for i := 0 to Count-1 do with TEvent(Items[i]) do begin
      if EMCValid and (FlowQual <= ConfigRec.MaxGoodQual) then begin
        SortList.AddValue(TotalFlow);
      end;
    end;
    Result := SortList.Median;
  finally
    SortList.Free;
  end;
end;

{------------------------------------------------------------------------------}

function TEventList.Compare(Item1,Item2: tObject): Integer;
var
  Event1,Event2: TEvent;
begin
  Event1 := TEvent(Item1);
  Event2 := TEvent(Item2);
  if Event1.Key < Event2.Key then Result := -1
  else if Event1.Key > Event2.Key then Result := 1
  else Result := 0;
end;

{------------------------------------------------------------------------------}

procedure TEventList.AssignEventIDs(Update: Boolean);
var
  EventSuffix: Char;
  Event,PrevEvent: TEvent;
  EventID: String;
  i: Integer;
begin
  for i := 0 to Count-1 do begin
    EventSuffix := 'A';
    Event := TEvent(Items[i]);
    if (i > 0) then begin
      PrevEvent := TEvent(Items[i-1]);
      if TToSDateOnly(Event.StartOfEvent) = TToSDateOnly(PrevEvent.StartOfEvent) then begin
        EventSuffix := SToC(iifStr(Update,PrevEvent.NewEventID,PrevEvent.EventID),FINALCHAR);
        if TToSTimeOnly(Event.StartOfEvent,True) <> TToSTimeOnly(PrevEvent.StartOfEvent,True) then begin
          EventSuffix := Chr(Ord(EventSuffix)+1);
        end;
      end;
    end;
    EventID := RelToTemplateTimeStr(Event.StartOfEvent,'YYYYMMDD')+EventSuffix;
    if Update then Event.NewEventID := EventID else Event.EventID := EventID;
  end;
end;

{------------------------------------------------------------------------------}

procedure TEventList.CalculateRainfallTotals(HyFile: THyFile);
var
  TempRain: TReal;
  IntensityQual,i: Integer;
begin
  for i := 0 to Count-1 do with TEvent(Items[i]) do begin
    if StartOfRain <> EndOfRain then begin
      GetValue(HyFile,RainVarFrom,RainVarTo,StartOfRain,EndOfRain,fTotalRain,fRainQual);
      GetValue(HyFile,IntensityVarFrom,IntensityVarTo,StartOfRain,EndOfRain,fIntensity,IntensityQual);
    end else begin
    { If there is no rain event, use the start and end of flow to compute the rain quality only }
      GetValue(HyFile,RainVarFrom,RainVarTo,StartOfFlow,EndOfFlow,TempRain,fRainQual);
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure TEventList.CalculateFlowTotals(HyFile: THyFile);
var
  RainEventParams: TRainEventParams;
  i: Integer;
begin
  for i := 0 to Count-1 do with TEvent(Items[i]) do begin
    if RainMode <> rmExtra then begin
      if StartOfFlow <> EndOfFlow then begin
        GetValue(HyFile,VolumeVarFrom,VolumeVarTo,StartOfFlow,EndOfFlow,fTotalFlow,fFlowQual);
      end else begin
      { If there is no flow event, use the start and end of rain to compute the flow volume }
        RainEventParams := TRainEventParams(RainEventParamsList.Find(HyFile.HSite,StartOfRain));
        GetValue(HyFile,VolumeVarFrom,VolumeVarTo,
          StartOfRain,
          TimeOffset (EndOfRain, iMinute, +RainEventParams.EndExtendInterval),
          fTotalFlow,fFlowQual);
      end;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure TEventList.CalculateStatistics(HyFile: THyFile; SiteDetails: TSiteDetails; All: Boolean);
var
  Event,PrevEvent: TEvent;
  Statistic: TEventStatistic;
  EventVariable: TEventVariable;
  Calculate: Boolean;
  i,j: Integer;
begin
  for i := 0 to Count-1 do begin
  { Get this event and previous event }
    Event := TEvent(Items[i]);
    if i > 0
      then PrevEvent := TEvent(Items[i-1])
      else PrevEvent := nil;

  { Compute all statistics for this event }
    if All then begin
      for j := Event.Statistics.Count-1 downto 0 do begin
        Statistic := TEventStatistic(Event.Statistics.Items[j]);
        EventVariable := EventVariableList.VarByNum(Statistic.VarNum);
        Calculate := False;
        if (EventVariable.VarCode = 'RV') then begin
          Calculate := Assigned(Event.RV);
        end else begin
          if Event.FlowValid then begin
            Calculate := True;
          end else if Event.EMCValid then begin
            Calculate := IsIn(EventVariable.VarCode,FlowStatistics);
          end;
        end;
        if Calculate
          then Statistic.Calculate(HyFile,EventVariable.VarCode,Event,PrevEvent,SiteDetails)
          else Event.Statistics.Delete(j);
      end;

  { Compute RV statistic only }
    end else begin
      if Assigned(Event.RV) then Event.RV.Calculate(HyFile,'RV',Event,PrevEvent,SiteDetails);
    end;

    if DispDelay(1) then Disp('Calculating statistics: '+StIdToS(SiteDetails.fSite)+' '+InSuffix+' '+TToS(Event.StartOfEvent,True));
  end;
end;

{------------------------------------------------------------------------------}

procedure TEventList.CalculateEMCs(HyFile: THyFile; MedianVolume: TReal);
var
  i: Integer;
begin
  for i := 0 to Count-1 do with TEvent(Items[i]) do begin
    if EMCValid and (FlowQual <= ConfigRec.MaxGoodQual) then begin
      EMCList.LoadPartitions(HyFile,TEvent(Items[i]));
      EMCList.Calculate(HyFile,MedianVolume);
      EMCList.WriteToList;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure TEventList.ExtendEvents;
var
  FlowEventParams: TFlowEventParams;
  RainEventParams: TRainEventParams;
  PrevEvent,NextEvent: TEvent;
  PrevEventEnd,NextEventStart: TRelTime;
  i: Integer;
begin
{ First pass to extend start of events }
  for i := 0 to Count-1 do with TEvent(Items[i]) do begin
    if RainMode = rmFlow then begin
      FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(evSite,StartOfFlow));
      StartOfStorm    := TimeOffset (StartOfFlow, iMinute, -FlowEventParams.StartExtendInterval);
    end else if RainMode = rmRain then begin
      StartOfStorm    := StartOfRain-iifInt(ProgMode = pmValidate,SORExtendInterval,0);
    end else if RainMode = rmBoth then begin
      FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(evSite,StartOfFlow));
      StartOfStorm    := Min(
        TimeOffset (StartOfFlow, iMinute, -FlowEventParams.StartExtendInterval),
        TimeOffset (StartOfRain, iMinute, -iifInt(ProgMode = pmValidate,SORExtendInterval,0))
        );
    end;

  { Check that the start of storm does not overlap with the end of the previous event }
    if (ProgMode = pmValidate) and (i > 0) then begin
      PrevEvent := TEvent(Items[i-1]);
      PrevEventEnd := 0;
      case PrevEvent.RainMode of
        rmFlow: PrevEventEnd := PrevEvent.EndOfFlow;
        rmRain: PrevEventEnd := PrevEvent.EndOfRain;
        rmBoth: PrevEventEnd := Max(PrevEvent.EndOfFlow,PrevEvent.EndOfRain);
      end;
      if StartOfStorm < PrevEventEnd then StartOfStorm := PrevEventEnd;
    end;
  end;

{ Second pass to extend end of events }
  for i := 0 to Count-1 do with TEvent(Items[i]) do begin
    if RainMode = rmFlow then begin
      FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(evSite,StartOfFlow));
      EndOfStorm      := TimeOffset (EndOfFlow, iMinute, +FlowEventParams.EndExtendInterval);
    end else if RainMode = rmRain then begin
      RainEventParams := TRainEventParams(RainEventParamsList.Find(evSite,StartOfRain));
      EndOfStorm      := TimeOffset (EndOfRain, iMinute, +RainEventParams.EndExtendInterval);
    end else if RainMode = rmBoth then begin
      FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(evSite,StartOfFlow));
      RainEventParams := TRainEventParams(RainEventParamsList.Find(evSite,StartOfRain));
      EndOfStorm      := Max(
        TimeOffset (EndOfFlow, iMinute, +FlowEventParams.EndExtendInterval),
        TimeOffset (EndOfRain, iMinute, +RainEventParams.EndExtendInterval));
    end;

  { Check that the end of the storm does not overlap with the start of the next event }
    if (ProgMode = pmValidate) and (i < Count-1) then begin
      NextEvent := TEvent(Items[i+1]);
      NextEventStart := NextEvent.StartOfStorm;
      if EndOfStorm > NextEventStart then EndOfStorm := NextEventStart;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure TEventList.LoadRecords(DB: TADODB; TableName: String; aSite: TStID);
var
  SQLStartTime,SQLEndTime,TimeQuery: String;
  Event: TEvent;
begin
  if StartTime <> EndTime then begin
    SQLStartTime := RelToTemplateTimeStr(StartTime,DateFormat);
    SQLEndTime   := RelToTemplateTimeStr(EndTime,DateFormat);
    TimeQuery    := ' AND '+
      '(((StartOfRain >= '+SQLStartTime+') AND (EndOfRain <= '+SQLEndTime+')) OR '+
      '((StartOfFlow >= '+SQLStartTime+') AND (EndOfFlow <= '+SQLEndTime+')))';
  end else begin
    TimeQuery    := '';
  end;
  with DB.ADOQuery do begin
    DB.AddSQL(
      'SELECT * FROM '+TableName+' '+
      'WHERE (Site = '''+StIDToS(aSite)+''') AND '+
            '(RainMode <> ''Extra'') AND '+
            '(Flow_Event_Valid = True)'+TimeQuery+' '+
      'ORDER BY EventID');
    Open;
    First;
    while not EOF do begin
      Event := TEvent.Create(SToStID(FieldByName('Site').AsString),
        FieldByName('Suffix').AsString,FieldByName('ID').AsInteger);
      with Event do begin
        EventID     := FieldByName('EventID').AsString;
        RainMode    := StrToRainMode(FieldByName('RainMode').AsString);
        if RainMode = rmBoth
          then RainModeNum := SToI(SToC(FieldByName('RainMode').AsString,FINALCHAR));
        if not StrIsNil(FieldByName('StartOfRain').AsString)
          then StartOfRain := DateTimeToRel(StrToDateTime(FieldByName('StartOfRain').AsString),0,True);
        if not StrIsNil(FieldByName('EndOfRain').AsString)
          then EndOfRain   := DateTimeToRel(StrToDateTime(FieldByName('EndOfRain').AsString),0,True);
        TotalRain   := FieldByName('TotalRain').AsFloat;
        Intensity   := FieldByName('Intensity').AsFloat;
        RainQual    := FieldByName('RainQual').AsInteger;
        FlowMode    := StrToFlowMode(FieldByName('FlowMode').AsString);
        if not StrIsNil(FieldByName('StartOfFlow').AsString)
          then StartOfFlow := DateTimeToRel(StrToDateTime(FieldByName('StartOfFlow').AsString),0,True);
        if not StrIsNil(FieldByName('EndOfFlow').AsString)
          then EndOfFlow   := DateTimeToRel(StrToDateTime(FieldByName('EndOfFlow').AsString),0,True);
        TotalFlow   := FieldByName('FlowVolume').AsFloat;
        FlowQual    := FieldByName('FlowQual').AsInteger;
        FlowValid   := FieldByName('Flow_Event_Valid').AsBoolean;
        EMCValid    := FieldByName('EMC_Event_Valid').AsBoolean;
      end;
      Add(Event);
      Next;
    end;
    Close;
  end;
end;

{------------------------------------------------------------------------------}

procedure TEventList.LoadAllRecords(DB: TADODB; TableName: String; aSite: TStID);
var
  PrevStartTime,PrevEndTime: TRelTime;
begin
  PrevStartTime := StartTime;
  PrevEndTime   := EndTime;
  try
    StartTime := 0;
    EndTime   := 0;
    LoadRecords(DB,TableName,aSite);
  finally
    StartTime := PrevStartTime;
    EndTime   := PrevEndTime;
  end;
end;

{------------------------------------------------------------------------------}

procedure TEventList.WriteToFile(aSite: TStID; Suffix: string; Variable: TVariable);
var
  OutputFile: TPutHyd;
  Event: TEvent;
  EventValid: Boolean;
  i: Integer;
begin
  if not VariableExists(aSite,Suffix,Variable) then begin
    OutputFile := TPutHyd.Create(tswmAppend,0,phPut);
    try
      if not TimesEQ (StartTime,EndTime) then
        OutputFile.AddPoint(aSite,Suffix,Variable,StartTime,0,1,dtrMean);
      for i := 0 to Count-1 do begin
        Event := TEvent(Items[i]);
        EventValid := False;
        case ProgMode of
          pmCompute:    EventValid := True;
          pmUpdate:     EventValid := Event.FlowValid;
          pmValidate:   EventValid := Event.FlowValid;
          pmStatistics: EventValid := Event.FlowValid;
          pmEMC:        EventValid := Event.EMCValid;
        end;
        if EventValid and (Event.RainMode in [rmNone,rmFlow,rmBoth]) then begin
          OutputFile.AddPoint(aSite,Suffix,Variable,Event.StartOfFlow,0,1,dtrMean);
          OutputFile.AddPoint(aSite,Suffix,Variable,Event.EndOfFlow,1,1,dtrMean);
        end;
      end;
      if not TimesEQ (StartTime,EndTime) then
        OutputFile.AddPoint(aSite,Suffix,Variable,EndTime,0,1,dtrMean);
    finally
      OutputFile.Free;
    end;
  end else begin
    DispPush([dspWarning]);
    try
      Disp('Event trace (variable '+VariableAsString(Variable)+') '+
        'already exists in '+StIDToS(aSite)+'.'+Suffix);
    finally
      DispPop;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure TEventList.WriteToList(Header: String; DoStatistics: Boolean);
var
  i: Integer;
begin
  if not StrIsNil(Header) then begin
    ListDev.Writeln(Header);
    ListDev.Writeln;
  end;
  for i := 0 to Count-1 do with TEvent(Items[i]) do begin
    ListDev.Writeln(DisplayText);
    if DoStatistics then Statistics.WriteToList;
  end;
  ListDev.Writeln;
end;

{------------------------------------------------------------------------------}

procedure TEventList.WriteStormsToList(Header: String);
var
  i: Integer;
begin
  if not StrIsNil(Header) then begin
    ListDev.Writeln(Header);
    ListDev.Writeln;
  end;
  for i := 0 to Count-1 do with TEvent(Items[i]) do begin
    ListDev.Writeln(TToS(StartOfStorm,False)+' '+TToS(EndOfStorm,False));
  end;
  ListDev.Writeln;
end;

{------------------------------------------------------------------------------}
{                              TSiteDetails object                             }
{------------------------------------------------------------------------------}

constructor TSiteDetails.Create(aSite: TStID; Area: TReal; AreaQual: Integer;
  HasRainEvent,HasFlowEvent,HasRV: Boolean; FlowVar: TVariable);
begin
  inherited Create;
  fSite         := aSite;
  fArea         := Area;
  fAreaQual     := AreaQual;
  fHasRainEvent := HasRainEvent;
  fHasFlowEvent := HasFlowEvent;
  fHasRV        := HasRV;
  fFlowVar      := FlowVar;
end;

function TSiteDetails.GetArea: TReal;
begin
{ Convert area in acres to square feet }
  Result := fArea * 43560;
end;

{------------------------------------------------------------------------------}
{                            TSiteDetailsList object                           }
{------------------------------------------------------------------------------}

constructor TSiteDetailsList.Create;
begin
  inherited Create;
  Sorted := True;
end;

function TSiteDetailsList.Compare(Item1,Item2: tObject): Integer;
begin
  Result := StIDCompare(TSiteDetails(Item1).sdSite,TSiteDetails(Item2).sdSite);
end;

function TSiteDetailsList.FindSite(aSite: TStID): TSiteDetails;
var
  i: Integer;
begin
  if Find(TSiteDetails.Create(aSite,0,0,False,False,False,NullVariable),i)
    then Result := TSiteDetails(Items[i])
    else Result := nil;
end;

procedure TSiteDetailsList.LoadRecords(DB: TADODB; TableName: String);
var
  FlowVariable: TVariable;
begin
  with DB.ADOQuery do begin
    DB.AddSQL('SELECT * FROM '+TableName);
    Open;
    First;
    while not EOF do begin
      try
        FlowVariable := VariableCompose(FieldByName('FlowVariable').AsString);
      except
        FlowVariable := NullVariable;
      end;
      Add(TSiteDetails.Create(
        SToStID(FieldByName('Site').AsString),
        FieldByName('DA').AsFloat,
        FieldByName('DA Qual').AsInteger,
        FieldByname('HasRainEvent').AsBoolean,
        FieldByName('HasFlowEvent').AsBoolean,
        FieldByName('HasRV').AsBoolean,
        FlowVariable));
      Next;
    end;
    Close;
  end;
end;

{------------------------------------------------------------------------------}
{                             TEventVariable object                            }
{------------------------------------------------------------------------------}

constructor TEventVariable.Create(VarNum: Integer; VarCode,Description: String);
begin
  inherited Create;
  fVarNum      := VarNum;
  fVarCode     := VarCode;
  fDescription := Description;
end;

{------------------------------------------------------------------------------}
{                            TEventVariableList object                         }
{------------------------------------------------------------------------------}

procedure TEventVariableList.LoadRecords(DB: TADODB; TableName: String);
begin
  with DB.ADOQuery do begin
    DB.AddSQL('SELECT * FROM '+TableName);
    Open;
    First;
    while not EOF do begin
      Add(TEventVariable.Create(
        FieldByName('RV_Var_No').AsInteger,
        FieldByName('RV_Code').AsString,
        FieldByName('Description').AsString));
      Next;
    end;
    Close;
  end;
end;

{------------------------------------------------------------------------------}

function TEventVariableList.VarByCode(VarCode: String): TEventVariable;
var
  EventVariable: TEventVariable;
  i: Integer;
begin
  Result := nil;
  for i := 0 to Count-1 do begin
    EventVariable := TEventVariable(Items[i]);
    if EventVariable.VarCode = VarCode then begin
      Result := EventVariable;
      Break;
    end;
  end;
end;

{------------------------------------------------------------------------------}

function TEventVariableList.VarByNum(VarNum: Integer): TEventVariable;
var
  EventVariable: TEventVariable;
  i: Integer;
begin
  Result := nil;
  for i := 0 to Count-1 do begin
    EventVariable := TEventVariable(Items[i]);
    if EventVariable.VarNum = VarNum then begin
      Result := EventVariable;
      Break;
    end;
  end;
end;

{------------------------------------------------------------------------------}
{                          TEventStatistic object                              }
{------------------------------------------------------------------------------}

constructor TEventStatistic.Create(ID,VarNum: Integer);
begin
  inherited Create;
  fID      := ID;
  fVarNum  := VarNum;
  fValue   := 0;
  fQuality := ConfigRec.DefBadQual;
end;

{------------------------------------------------------------------------------}

function TEventStatistic.GetFieldNames: String;
begin
  with fFieldNames do begin
    Clear;
    Add('ID');
    Add('RV_Var_No');
    Add('[Value]'); { "value" is a reserved word in Access, so must be enclosed in brackets }
    Add('Quality');
  end;
  Result := inherited GetFieldNames;
end;

{------------------------------------------------------------------------------}

function TEventStatistic.GetFieldValues: String;
begin
  with fFieldValues do begin
    Clear;
    Add(IntToStr(fID));
    Add(IntToStr(fVarNum));
    Add(iifStr(fQuality <= ConfigRec.MaxGoodQual,RToS(fValue,10,3,fsFixed),'NULL'));
    Add(IntToStr(fQuality));
  end;
  Result := inherited GetFieldValues;
end;

{------------------------------------------------------------------------------}

function TEventStatistic.GetWhere: String;
begin
  with fWhere do begin
    Clear;
    Add('ID = '+IntToStr(fID));
    Add('RV_Var_No = '+IntToStr(fVarNum));
  end;
  Result := inherited GetWhere;
end;

{------------------------------------------------------------------------------}

function TEventStatistic.GetSetList: String;
begin
  with fSetList do begin
    Clear;
    Add('[Value] = '+iifStr(fQuality <= ConfigRec.MaxGoodQual,RToS(fValue,10,3,fsFixed),'NULL')); { "value" is a reserved word }
    Add('Quality = '+IntToStr(fQuality));
  end;
  Result := inherited GetSetList;
end;

{------------------------------------------------------------------------------}

function TEventStatistic.GetDisplayText: String;
var
  EventVariable: TEventVariable;
begin
  EventVariable := EventVariableList.VarByNum(fVarNum);
  if Assigned(EventVariable) then with EventVariable do begin
    Result := StrPadRight(VarCode,8,' ')+' '+
              StrPadRight(Description,40,' ')+' '+
              iifStr(fQuality <= ConfigRec.MaxGoodQual,RToS(fValue,10,3,fsFixed),StrReplicate(' ',10))+' '+
              IntToStr(fQuality);
  end;
end;

{------------------------------------------------------------------------------}

procedure TEventStatistic.Calculate(HyFile: THyFile; VarCode: String; Event,PrevEvent: TEvent; SiteDetails: TSiteDetails);
var
  EventStart,EventEnd: TRelTime;
  Volume,Rain,Intensity: TReal;
  VolumeQual,RainQual,IntensityQual,Interval: Integer;
  Time,StartPeriod,EndPeriod: TRelTime;
  FlowEventParams: TFlowEventParams;
  RainEventParams: TRainEventParams;
begin
{ Determine event start and end for flow statistics }
  EventStart := 0;
  EventEnd   := 0;
  if IsIn(VarCode,FlowStatistics+',RV') then begin
    if (Event.StartOfFlow <> 0) and (Event.EndOfFlow <> 0) then begin
      EventStart := Event.StartOfFlow;
      EventEnd   := Event.EndOfFlow;
    end else begin
      FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(HyFile.HSite,Event.StartOfRain));
      EventStart := Event.StartOfRain;
      EventEnd   := TimeOffset (Event.EndOfRain, iMinute, +FlowEventParams.EndExtendInterval);
    end;
  end;

{ Runoff Coefficient }
  if VarCode = 'RV' then begin
    GetValue(HyFile,VolumeVarFrom,VolumeVarTo,EventStart,EventEnd,Volume,VolumeQual);
    GetValue(HyFile,RainVarFrom,RainVarTo,Event.StartOfRain,Event.EndOfRain,Rain,RainQual);
    if (SiteDetails.Area <> 0) and (Rain <> 0) then begin
      fValue := ((Volume/SiteDetails.Area)*12)/Rain;
    end;
    fQuality := Max(Max(VolumeQual,RainQual),SiteDetails.AreaQual);

{ Total Flow }
  end else if VarCode = 'TQ' then begin
    GetValue(HyFile,VolumeVarFrom,VolumeVarTo,EventStart,EventEnd,fValue,fQuality);

{ Total Rainfall }
  end else if VarCode = 'TR' then begin
    GetValue(HyFile,RainVarFrom,RainVarTo,Event.StartOfRain,Event.EndOfRain,fValue,fQuality);

{ Peak Flowrate, Time to PQ }
  end else if IsInList('PQ,T_PQ',VarCode) then with HyFile do begin
    LoadHyValueVars(FlowVarFrom,FlowVarTo);
    LoadHyValueTimes(EventStart,EventEnd);
    Aggregate(False);
    if VarCode = 'PQ'
      then fValue := HyValue.DMax
      else fValue := HyValue.MaxTime-EventStart;
    fQuality := HyValue.QMax;

{ Peak Rainfall Intensity, Time to PRI }
  end else if Pos('PRI',VarCode) <> 0 then begin
    if (Event.StartOfRain <> 0) and (Event.EndOfRain <> 0) then begin
      Interval := SToI(StrRight(VarCode,Length(VarCode)-Pos('-',VarCode)));
      Time := Event.StartOfRain;
      while Time < Event.EndOfRain do begin
        StartPeriod := Time;
        EndPeriod   := TimeOffset (Time,iMinute,Interval);
        GetValue(HyFile,IntensityVarFrom,IntensityVarTo,StartPeriod,EndPeriod,Intensity,IntensityQual);
        if Intensity > fValue then begin
          if StrLeft(VarCode,3) = 'PRI'
            then fValue := Intensity
            else fValue := StartPeriod-Event.StartOfRain;
          fQuality := IntensityQual;
        end;
        RainEventParams := TRainEventParams(RainEventParamsList.Find(HyFile.HSite,Time));
        Time := TimeOffset (Time,iMinute,RainEventParams.SiteInterval);
      end;
    end;

{ Preceding Dry Interval Time }
  end else if VarCode = 'PDIT' then begin
    if Assigned(PrevEvent) then begin
      if (Event.StartOfRain <> 0) and (PrevEvent.EndOfRain <> 0) then begin
        fValue   := Event.StartOfRain-PrevEvent.EndOfRain;
        fQuality := ConfigRec.DefGoodQual;
      end;
    end;

{ Preceding Dry Interval Rainfall }
  end else if VarCode = 'PDIR' then begin
    if Assigned(PrevEvent) then begin
      if (Event.StartOfRain <> 0) and (PrevEvent.EndOfRain <> 0) then begin
        GetValue(HyFile,RainVarFrom,RainVarTo,PrevEvent.EndOfRain,Event.StartOfRain,fValue,fQuality);
      end;
    end;

{ Preceding Event Total Rainfall }
  end else if VarCode = 'PETR' then begin
    if Assigned(PrevEvent) then begin
      if (PrevEvent.StartOfRain <> 0) and (PrevEvent.EndOfRain <> 0) then begin
        GetValue(HyFile,RainVarFrom,RainVarTo,PrevEvent.StartOfRain,PrevEvent.EndOfRain,fValue,fQuality);
      end;
    end;

{ Time to Centroid of Rainfall }
  end else if VarCode = 'T_CR' then begin
    if (Event.StartOfRain <> 0) and (Event.EndOfRain <> 0) then begin
      RainEventParams := TRainEventParams(RainEventParamsList.Find(HyFile.HSite,Event.StartOfRain));
      GetCentroid(HyFile,RainVarFrom,RainVarTo,Event.StartOfRain,Event.EndOfRain,RainEventParams.SiteInterval,fValue,fQuality);
    end;

{ Time to Mid-point of Rainfall }
  end else if VarCode = 'T_50R' then begin
    if (Event.StartOfRain <> 0) and (Event.EndOfRain <> 0) then begin
      RainEventParams := TRainEventParams(RainEventParamsList.Find(HyFile.HSite,Event.StartOfRain));
      GetMidPoint(HyFile,RainVarFrom,RainVarTo,Event.StartOfRain,Event.EndOfRain,RainEventParams.SiteInterval,fValue,fQuality);
    end;

{ Time to Centroid of Flow }
  end else if VarCode = 'T_CQ' then begin
    FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(HyFile.HSite,EventStart));
    GetCentroid(HyFile,FlowVarFrom,FlowVarTo,EventStart,EventEnd,FlowEventParams.SiteInterval,fValue,fQuality);

{ Time to Mid-point of Flow }
  end else if VarCode = 'T_50Q' then begin
    FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(HyFile.HSite,EventStart));
    GetMidPoint(HyFile,VolumeVarFrom,VolumeVarTo,EventStart,EventEnd,FlowEventParams.SiteInterval,fValue,fQuality);
  end;
end;

{------------------------------------------------------------------------------}
{                       TEventStatisticList object                             }
{------------------------------------------------------------------------------}

constructor TEventStatisticList.Create(ID: Integer);
var
  EventVariable: TEventVariable;
  EventStatistic: TEventStatistic;
  i: Integer;
begin
  inherited Create;
  Sorted := True;
  for i := 0 to EventVariableList.Count-1 do begin
    EventVariable  := TEventVariable(EventVariableList.Items[i]);
    EventStatistic := TEventStatistic.Create(ID,EventVariable.VarNum);
    Add(EventStatistic);
  end;
end;

function TEventStatisticList.Compare(Item1,Item2: tObject): Integer;
begin
  Result := StrCompare(IntToStr(TEventStatistic(Item1).VarNum),IntToStr(TEventStatistic(Item2).VarNum),True);
end;

function TEventStatisticList.Find(VarCode: String): TEventStatistic;
var
  EventVariable: TEventVariable;
  EventStatistic: TEventStatistic;
  i: Integer;
begin
  Result := nil;
  EventVariable := EventVariableList.VarByCode(VarCode);
  if Assigned(EventVariable) then begin
    EventStatistic := TEventStatistic.Create(0,EventVariable.VarNum);
    if inherited Find(EventStatistic,i) then Result := TEventStatistic(Items[i]);
  end;
end;

procedure TEventStatisticList.WriteToList;
var
  i: Integer;
begin
  for i := 0 to Count-1 do begin
    ListDev.Writeln('  '+TEventStatistic(Items[i]).DisplayText);
  end;
  ListDev.Writeln;
end;

{------------------------------------------------------------------------------}
{                                TPoint object                                 }
{------------------------------------------------------------------------------}

constructor TPoint.Create(Site: TStID; Variable: TVariable; Time: TRelTime; Value: TReal; Quality: Integer; Comment: String);
begin
  inherited Create;
  fSite     := Site;
  fVariable := Variable;
  fTime     := Time;
  fValue    := Value;
  fQuality  := Quality;
  fComment  := TStringList.Create;
  fComment.Add(Comment);
end;

destructor TPoint.Destroy;
begin
  fComment.Free;
  inherited Destroy;
end;

{------------------------------------------------------------------------------}
{                              TPointList object                               }
{------------------------------------------------------------------------------}

constructor TPointList.Create;
begin
  inherited Create(100,True,False); { Sorted, no duplicates }
end;

function TPointList.Compare(Item1,Item2: TObject): Integer;
var
  Point1,Point2: TPoint;
begin
  Result := 0;
  Point1 := TPoint(Item1);
  Point2 := TPoint(Item2);
  if fSampleType = stEMC then begin
    Result := VariableComp(Point1.Variable,Point2.Variable);
  end;
  if Result = 0 then begin
    if Point1.Time > Point2.Time then Result := 1
    else if Point1.Time < Point2.Time then Result := -1
    else Result := 0;
  end;
end;

{------------------------------------------------------------------------------}

procedure TPointList.LoadWQPoints(DB: TADODB; aSite: TStID; StartTime,EndTime: TRelTime; SampleType: TSampleType);
var
  Variable: TVariable;
  Time: TRelTime;
  Value: TReal;
  Quality: Integer;
  Where,Query,Comment: String;
  COCTable,VarTable,SampleTable,ParamsTable,ResultTable,EventTable: String;
begin
  fSampleType := SampleType;
  COCTable    := TableNames.Values['Chain of Custody'];        { COC }
  ParamsTable := TableNames.Values['WQM Parameter Index'];     { WQM_Parameters }
  SampleTable := TableNames.Values['Sample Initialize Match']; { COA_SAMP }
  ResultTable := TableNames.Values['Lab Sample Results'];      { WQM_Result }
  VarTable    := TableNames.Values['EMC Variables'];           { EMC_Variables }
  EventTable  := TableNames.Values['Event Output'];            { FLOW_EVENT }

  Clear;
  if SampleType in [stEMC,stStormWater,stExcluded,stBaseFlow] then begin
    Where := '(('+COCTable+'.SITE = '''+StIDToS(aSite)+''') AND ';
    case SampleType of
      stEMC:        Where := Where+'('+COCTable+'.EXCLUDE = False) AND ('+EventTable+'.EMC_Event_Valid = True)';
      stStormWater: Where := Where+'('+COCTable+'.EXCLUDE = False)';
      stExcluded:   Where := Where+'('+COCTable+'.EXCLUDE = True) AND ('+COCTable+'.SAMPLE_MEDIUM <> ''Baseflow'')';
      stBaseFlow:   Where := Where+'('+COCTable+'.EXCLUDE = True) AND ('+COCTable+'.SAMPLE_MEDIUM = ''Baseflow'')';
    end;
    Where := Where+') ';
    Query := 'SELECT '+COCTable+'.SITE,'+COCTable+'.WQM_SAMPLE_ID,'+
                       COCTable+'.COMPOSITE_NO,'+COCTable+'.EXCLUDE,'+
                       ParamsTable+'.VARNUM,'+COCTable+'.ALT_DATE,'+
                       SampleTable+'.FLOW_DATE,'+COCTable+'.SAMPLER_DATE,'+
                       ResultTable+'.EDITED_VALUE,'+iifStr(SampleType = stEMC,
                       ResultTable+'.HYDRON_QUALITY',COCTable+'.HYDRON_QUALITY')+' '+
            'FROM ((('+ResultTable+' INNER JOIN '+ParamsTable+' ON '+
                       ResultTable+'.WQM_Parameter_No = '+ParamsTable+'.WQM_Parameter_No) '+
        'INNER JOIN ('+COCTable+' LEFT JOIN '+SampleTable+' ON '+
                       COCTable+'.SAMP_NO = '+SampleTable+'.SAMP_NO) ON '+
                       ResultTable+'.WQM_SAMPLE_NO = '+COCTable+'.WQM_SAMPLE_NO) '+
         'INNER JOIN '+VarTable+' ON '+ParamsTable+'.VARNUM = '+VarTable+'.EMC_VARNUM) '+
    iifStr(SampleType <> stExcluded,'INNER JOIN '+EventTable+' ON '+COCTable+'.EVENT_ID = '+EventTable+'.EventID ','')+
              'WHERE '+Where+
           'ORDER BY '+ParamsTable+'.VARNUM,'+COCTable+'.ALT_DATE,'+
                       SampleTable+'.FLOW_DATE,'+COCTable+'.SAMPLER_DATE';
  end else begin
    Where := '(('+SampleTable+'.SITE = '''+StIDToS(aSite)+''') AND ';
    case SampleType of
      stPrimary:   Where := Where+'('+SampleTable+'.HYDRON_VAR = 350.10)';
      stSecondary: Where := Where+'('+SampleTable+'.HYDRON_VAR = 350.11)';
    end;
    Where := Where+') ';
    Query := 'SELECT '+SampleTable+'.SITE,'+SampleTable+'.HYDRON_VAR,'+
                       SampleTable+'.FLOW_DATE,'+SampleTable+'.QUAL_CODE,'+
                       SampleTable+'.BOTTLE,'+COCTable+'.WQM_SAMPLE_ID '+
               'FROM '+SampleTable+' LEFT JOIN '+COCTable+' ON '+
                       SampleTable+'.COC_NO = '+COCTable+'.COC_NO '+
              'WHERE '+Where+
           'ORDER BY '+SampleTable+'.SITE,'+SampleTable+'.HYDRON_VAR,'+
                       SampleTable+'.FLOW_DATE';
  end;

  with DB.ADOQuery do begin
    DB.AddSQL(Query);
    Open;
    First;
    while not EOF do begin

    { Determine sample time }
      if SampleType in [stEMC,stStormWater,stExcluded,stBaseFlow] then begin
        Time := 0;
        {PRAGMA IGNORE AsDateTime}
        if FieldByName('ALT_DATE').AsDateTime > 0 then begin
          {PRAGMA IGNORE AsDateTime}
          Time := DateTimeToRel(FieldByName('ALT_DATE').AsDateTime,0,False);
        {PRAGMA IGNORE AsDateTime}
        end else if FieldByName('FLOW_DATE').AsDateTime > 0 then begin
          {PRAGMA IGNORE AsDateTime}
          Time := DateTimeToRel(FieldByName('FLOW_DATE').AsDateTime,0,False);
        {PRAGMA IGNORE AsDateTime}
        end else if FieldByName('SAMPLER_DATE').AsDateTime > 0 then begin
          {PRAGMA IGNORE AsDateTime}
          Time := DateTimeToRel(FieldByName('SAMPLER_DATE').AsDateTime,0,False);
        end;
      end else begin
        {PRAGMA IGNORE AsDateTime}
        Time := DateTimeToRel(FieldByName('FLOW_DATE').AsDateTime,0,False);
      end;

    { Add water quality sample to list }
      if TimesEQ (StartTime,EndTime) or ((Time >= StartTime) and (Time <= EndTime)) then begin
        if SampleType in [stEMC,stStormWater,stExcluded,stBaseFlow] then begin
          Variable := VariableCompose(FieldByName('VARNUM').AsFloat);
          Value    := FieldByName('EDITED_VALUE').AsFloat;
          Quality  := FieldByName('HYDRON_QUALITY').AsInteger;
          Comment  := 'CN='+FieldByName('COMPOSITE_NO').AsString+' '+
                      'ID='+FieldByName('WQM_SAMPLE_ID').AsString;
        end else begin
          Variable := VariableCompose(FieldByName('HYDRON_VAR').AsFloat);
          Value    := 0;
          Quality  := FieldByName('QUAL_CODE').AsInteger;
          Comment  := 'BN='+FieldByName('BOTTLE').AsString+' '+
                      'ID='+FieldByName('WQM_SAMPLE_ID').AsString;
        end;
        if Quality <= ConfigRec.MaxGoodQual then begin
          Add(TPoint.Create(aSite,Variable,Time,Value,Quality,Comment));
        end;

        if DispDelay(1) then Disp('Loading '+SampleTypeToStr(SampleType)+' WQ samples: '+
          StIDToS(aSite)+' '+VariableAsString(Variable)+' '+TToS(Time,True));
      end;
      Next;
    end;
    Close;
  end;
end;

{------------------------------------------------------------------------------}

procedure TPointList.LoadTSPoints(HyFile: THyFile; Variable: TVariable; StartTime,EndTime: TRelTime);
const
  IgnoreComments = 'First line processed,Last line processed,Created by HYFL4';
var
  Time: TRelTime;
  Value: TReal;
  Quality,i: Integer;
  Comment: TStringList;
  Point: TPoint;
begin
  Comment := TStringList.Create;
  with HyFile do try
    if TimesEQ (StartTime,EndTime) then TimeBounds(Variable,StartTime,EndTime);
    Time := StartTime-relSecond;
    GetValue(Variable,Time,Value,Quality);
    NextValue(Variable,Time,Value,Quality);
    GetComments(Variable,Time,Comment);
    while Time <= EndTime do begin
      Point := TPoint.Create(HyFile.HSite,Variable,Time,Value,Quality,'');
      for i := 0 to Comment.Count-1 do begin
        if not IsInList(IgnoreComments,Comment[i]) then begin
          Point.Comment.Add(Comment[i]);
        end;
      end;
      Add(Point);
      NextValue(Variable,Time,Value,Quality);
      GetComments(Variable,Time,Comment);
    end;
  finally
    Comment.Free;
  end;
end;

{------------------------------------------------------------------------------}

procedure TPointList.AddEvent(Event: TEvent);

  procedure AddPoint(Point: TPoint; EventStart: Boolean; InsertType: TInsertType);
  var
    FlowEventParams: TFlowEventParams;
    BaseFlowParams: TBaseFlowParams;
    PointTime,GapTime: TRelTime;
    CommentStr: String;
    InsertBaseFlow: Boolean;
  begin
    with Event do begin
      if EventStart then begin
        FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(evSite,StartOfFlow));
        PointTime  := StartOfFlow;
        GapTime    := StartOfFlow-FlowEventParams.SiteInterval;
        CommentStr := 'Start '+EventID;
      end else begin
        FlowEventParams := TFlowEventParams(FlowEventParamsList.Find(evSite,EndOfFlow));
        PointTime  := EndOfFlow;
        GapTime    := EndOfFlow+FlowEventParams.SiteInterval;
        CommentStr := 'End '+EventID;
      end;
    end;
    with Point do begin
      if InsertType = itPointValue then begin
        InsertBaseFlow := False;
        BaseFlowParams := BaseFlowParamsList.Find(Site,Variable);
        if Assigned(BaseFlowParams) then begin
          InsertBaseFlow :=
            ((BaseFlowParams.fPositive) and (BaseFlowParams.fValue < Value)) or
            ((not BaseFlowParams.fPositive) and (BaseFlowParams.fValue > Value));
        end;
        if InsertBaseFlow then begin
          Add(TPoint.Create(Site,Variable,PointTime,BaseFlowParams.fValue,BaseFlowParams.fQuality,CommentStr));
        end else begin
          Add(TPoint.Create(Site,Variable,PointTime,Value,Quality,CommentStr));
        end;
      end else if InsertType = itZeroValue then begin
        Add(TPoint.Create(Site,Variable,PointTime,0,1,CommentStr));
      end else if InsertType = itCommentOnly then begin
        Comment.Insert(0,CommentStr);
      end;
      Add(TPoint.Create(Site,Variable,GapTime,0,201,''));
    end;
  end;

var
  CurrPoint,PrevPoint: TPoint;
  i: Integer;
begin
  with Event do begin
    for i := Count-1 downto 0 do begin
      CurrPoint := TPoint(Items[i]);
      if i = 0
        then PrevPoint := TPoint(Items[Count-1])
        else PrevPoint := TPoint(Items[i-1]);

    { If we have two points with the same variable }
      if VariablesEQ(CurrPoint.Variable,PrevPoint.Variable) and (i <> 0) then begin
        if (StartOfFlow < CurrPoint.Time) and (StartOfFlow > PrevPoint.Time) then begin
          if (EndOfFlow < CurrPoint.Time) and (EndOfFlow > PrevPoint.Time) then begin
            AddPoint(CurrPoint,True,itZeroValue);   { Add start of event with no samples }
          end else begin
            AddPoint(CurrPoint,True,itPointValue);  { Add start of event }
          end;
        end else if Req(StartOfFlow,CurrPoint.Time) then begin
          AddPoint(CurrPoint,True,itCommentOnly);   { Add start of event to existing WQ sample }
        end;

        if (EndOfFlow < CurrPoint.Time) and (EndOfFlow > PrevPoint.Time) then begin
          if (StartOfFlow < CurrPoint.Time) and (StartOfFlow > PrevPoint.Time) then begin
            AddPoint(PrevPoint,False,itZeroValue);  { Add end of event with no samples }
          end else begin
            AddPoint(PrevPoint,False,itPointValue); { Add end of event }
          end;
        end else if Req(EndOfFlow,CurrPoint.Time) then begin
          AddPoint(CurrPoint,False,itCommentOnly);  { Add end of event to existing WQ sample }
        end;

    { Otherwise we are at the end of one trace and the start of the next }
      end else begin
        if (StartOfFlow < CurrPoint.Time) and (EndOfFlow > CurrPoint.Time) then begin
          AddPoint(CurrPoint,True,itPointValue);    { Add start of event only if it overlaps with WQ samples }
        end;
        if (EndOfFlow > PrevPoint.Time) and (StartOfFlow < PrevPoint.Time) then begin
          AddPoint(PrevPoint,False,itPointValue);   { Add end of event only if it overlaps with WQ samples }
        end;
      end;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure TPointList.AddPoint(Point: TPoint);
var
  OldVariable: TVariable;
  TSComment: TStringList;
  CurrPoint,PrevPoint: TPoint;
  Value: TReal;
  Quality,Index,i: Integer;
begin
{ Change variable for searching }
  OldVariable := Point.Variable;
  Point.Variable := FlowVarFrom;

{ If point is found, then add comment to existing point }
  if Find(Point,Index) then begin
    TSComment := TPoint(Items[Index]).Comment;
    for i := 0 to Point.Comment.Count-1 do begin
      if TSComment.IndexOf(Point.Comment[i]) = -1 then TSComment.Add(Point.Comment[i]);
    end;

{ If point is not found, then add it, interpolating a value between the two surrounding points }
  end else begin
    for i := Count-1 downto 1 do begin
      CurrPoint := TPoint(Items[i]);
      PrevPoint := TPoint(Items[i-1]);
      if (Point.Time > PrevPoint.Time) and (Point.Time < CurrPoint.Time) then begin
        Value   := Interpolate(Point.Time,PrevPoint.Time,PrevPoint.Value,CurrPoint.Time,CurrPoint.Value,1);
        Quality := Max(PrevPoint.Quality,CurrPoint.Quality);
        Add(TPoint.Create(Point.Site,Point.Variable,Point.Time,Value,Quality,Point.Comment[0]));
      end;
    end;
  end;

{ Restore variable }
  Point.Variable := OldVariable;
end;

{------------------------------------------------------------------------------}

procedure TPointList.WriteToFile(aSite: TStID; Suffix: string; SampleVariable: TVariable);
var
  Action: TPHAction;
  DataTrans: TDataTrans;
  OutputFile: TPutHyd;
  i,j: Integer;
begin
  if VariablesEQ(SampleVariable,NullVariable) then begin
    Action    := phPut;
    DataTrans := dtrInst;
  end else begin
    Action    := phAdd;
    DataTrans := dtrITot;
  end;

  OutputFile := TPutHyd.Create(tswmAppend,0,Action);
  try
    for i := 0 to Count-1 do with TPoint(Items[i]) do begin
      if VariablesEQ(SampleVariable,NullVariable) then begin
        OutputFile.AddPoint(aSite,Suffix,Variable,Time,Value,Quality,DataTrans);
      end else begin
        OutputFile.AddPoint(aSite,Suffix,SampleVariable,Time,1,Quality,DataTrans);
      end;
      for j := 0 to Comment.Count-1 do OutputFile.AddComment(Comment[j]);
    end;
  finally
    OutputFile.Free;
  end;
end;

{------------------------------------------------------------------------------}
{                           TEMCVariable object                                }
{------------------------------------------------------------------------------}

constructor TEMCVariable.Create(VariableID: Integer; VariableName: String;
  EMCVar,LoadVar: TVariable; EMCUnits,LoadUnits,VolumeUnits: String; Conversion: TReal);
begin
  inherited Create;
  fVariableID   := VariableID;
  fVariableName := VariableName;
  fEMCVar       := EMCVar;
  fLoadVar      := LoadVar;
  fEMCUnits     := EMCUnits;
  fLoadUnits    := LoadUnits;
  fVolumeUnits  := VolumeUnits;
  fConversion   := Conversion;
end;

{------------------------------------------------------------------------------}
{                          TEMCVariableList object                             }
{------------------------------------------------------------------------------}

procedure TEMCVariableList.LoadRecords(DB: TADODB; TableName: String);
begin
  with DB.ADOQuery do begin
    DB.AddSQL('SELECT * FROM '+TableName);
    Open;
    First;
    while not EOF do begin
      Add(TEMCVariable.Create(
        FieldByName('EMC_VAR_NO').AsInteger,
        FieldByName('PARAMETER_CODE').AsString,
        VariableCompose(FieldByName('EMC_VARNUM').AsString),
        VariableCompose(FieldByName('LOAD_VARNUM').AsString),
        FieldByName('EMC_UNIT').AsString,
        FieldByName('LOAD_UNIT').AsString,
        'CF', { Hardcoded volume units }
        FieldByName('CONVERSION').AsFloat));
      Next;
    end;
    Close;
  end;
end;

{------------------------------------------------------------------------------}

function TEMCVariableList.Find(Variable: TVariable): TEMCVariable;
var
  EMCVariable: TEMCVariable;
  i: Integer;
begin
  Result := nil;
  for i := 0 to Count-1 do begin
    EMCVariable := TEMCVariable(Items[i]);
    if VariablesEQ(EMCVariable.EMCVar,Variable) then begin
      Result := EMCVariable;
      Break;
    end;
  end;
end;

{------------------------------------------------------------------------------}
{                           TEMCPartition object                               }
{------------------------------------------------------------------------------}

constructor TEMCPartition.Create(EMC: TEMC; ID: Integer; SampleID: String; Composite: Integer;
  VarFrom,VarTo: TVariable; StartTime,EndTime: TRelTime);
begin
  inherited Create;
  fEMC              := EMC;
  fID               := ID;
  fSampleID         := SampleID;
  fComposite        := Composite;
  fVarFrom          := VarFrom;
  fVarTo            := VarTo;
  fStartTime        := StartTime;
  fEndTime          := EndTime;
  fCumLoad          := 0;
  fCumVolume        := 0;
  fIncLoad          := 0;
  fIncVolume        := 0;
  fStartFlowRate    := 0;
  fEndFlowRate      := 0;
  fConcentration    := 0;
  fCumLoadQuality   := ConfigRec.DefBadQual;
  fCumVolumeQuality := ConfigRec.DefBadQual;
  fIncLoadQuality   := ConfigRec.DefBadQual;
  fIncVolumeQuality := ConfigRec.DefBadQual;
end;

{------------------------------------------------------------------------------}

function TEMCPartition.GetKey: String;
begin
  Result := VariableAsString(fVarTo)+
            RelToTemplateTimeStr(fStartTime,'YYYYMMDDHHIIEE')+
            RelToTemplateTimeStr(fEndTime,'YYYYMMDDHHIIEE');
end;

{------------------------------------------------------------------------------}

function TEMCPartition.GetFieldNames: String;
begin
  with fFieldNames do begin
    Clear;
    Add('ID');
    Add('EMC_Var_No');
    Add('Partition_ID');
    Add('Site');
    Add('Event_ID');
    Add('Parameter_Code');
    Add('Composite_No');
    Add('Sample_ID');
    Add('Start_Date');
    Add('End_Date');
    Add('Load_Unit');
    Add('Cum_Load');
    Add('Cum_Load_Qual');
    Add('Cum_Volume');
    Add('Cum_Volume_Qual');
    Add('Inc_Load');
    Add('Inc_Load_Qual');
    Add('Inc_Volume');
    Add('Inc_Volume_Qual');
    Add('Partition_Concentration');
    Add('Partition_Conc_Unit');
    Add('Start_Flowrate');
    Add('End_Flowrate');
  end;
  Result := inherited GetFieldNames;
end;

{------------------------------------------------------------------------------}

function TEMCPartition.GetFieldValues: String;
begin
  with fFieldValues do begin
    Clear;
    Add(IntToStr(fEMC.fEvent.ID));
    Add(IntToStr(fEMC.fEMCVariable.VariableID));
    Add(IntToStr(fID));
    Add(Quote(StIDToS(fEMC.fEvent.evSite)));
    Add(Quote(fEMC.fEvent.EventID));
    Add(Quote(fEMC.fEMCVariable.VariableName));
    Add(IntToStr(fComposite));
    Add(Quote(fSampleID));
    Add(RelToTemplateTimeStr(fStartTime,DateFormat));
    Add(RelToTemplateTimeStr(fEndTime,DateFormat));
    Add(Quote(fEMC.fEMCVariable.LoadUnits));
    Add(RToS(fCumLoad,7,2,fsFixed));
    Add(IntToStr(fCumLoadQuality));
    Add(RToS(fCumVolume,7,2,fsFixed));
    Add(IntToStr(fCumVolumeQuality));
    Add(RToS(fIncLoad,7,2,fsFixed));
    Add(IntToStr(fIncLoadQuality));
    Add(RToS(fIncVolume,7,2,fsFixed));
    Add(IntToStr(fIncVolumeQuality));
    Add(RToS(fConcentration,7,2,fsFixed));
    Add(Quote(fEMC.fEMCVariable.EMCUnits));
    Add(RToS(fStartFlowRate,7,2,fsFixed));
    Add(RToS(fEndFlowRate,7,2,fsFixed));
  end;
  Result := inherited GetFieldValues;
end;

{------------------------------------------------------------------------------}

function TEMCPartition.GetFieldTypes: String;
begin
  with fFieldTypes do begin
    Clear;
    Add('ID INTEGER');
    Add('EMC_Var_No INTEGER');
    Add('Partition_ID INTEGER');
    Add('Site VARCHAR');
    Add('Event_ID VARCHAR');
    Add('Parameter_Code VARCHAR');
    Add('Composite_No INTEGER');
    Add('Sample_ID VARCHAR');
    Add('Start_Date DATETIME');
    Add('End_Date DATETIME');
    Add('Load_Unit VARCHAR');
    Add('Cum_Load FLOAT');
    Add('Cum_Load_Qual INTEGER');
    Add('Cum_Volume FLOAT');
    Add('Cum_Volume_Qual INTEGER');
    Add('Inc_Load FLOAT');
    Add('Inc_Load_Qual INTEGER');
    Add('Inc_Volume FLOAT');
    Add('Inc_Volume_Qual INTEGER');
    Add('Partition_Concentration FLOAT');
    Add('Partition_Conc_Unit VARCHAR');
    Add('Start_Flowrate FLOAT');
    Add('End_Flowrate FLOAT');
  end;
  Result := inherited GetFieldTypes;
end;

{------------------------------------------------------------------------------}

function TEMCPartition.GetDisplayText: String;
begin
  Result := StrPadRight (StIDToS(fEMC.fEvent.evSite),SiteIdLen,' ')+' '+
            StrPadRight(fEMC.fEvent.EventID,9,' ')+' '+
            IToS(fComposite,3)+' '+
            StrPadRight(fSampleID,16,' ')+' '+
            VariableAsString(fVarTo)+' '+
            TToS(fStartTime,True)+' '+
            TToS(fEndTime,True);
end;

{------------------------------------------------------------------------------}

procedure TEMCPartition.Calculate(HyFile: THyFile);
var
  Quality: Integer;
  LoadUnits,VolumeUnits,ConcUnits: TUnitCode;
begin
{ Cumulative load }
  GetValue(HyFile,fVarFrom,fVarTo,fEMC.fEvent.StartOfFlow,fEndTime,fCumLoad,fCumLoadQuality);
{ Incremental load }
  GetValue(HyFile,fVarFrom,fVarTo,fStartTime,fEndTime,fIncLoad,fIncLoadQuality);
{ Cumulative volume }
  GetValue(HyFile,fVarFrom,VolumeVarTo,fEMC.fEvent.StartOfFlow,fEndTime,fCumVolume,fCumVolumeQuality);
{ Incremental volume }
  GetValue(HyFile,fVarFrom,VolumeVarTo,fStartTime,fEndTime,fIncVolume,fIncVolumeQuality);
{ Flow rate }
  GetValue(HyFile,fVarFrom,fVarFrom,fStartTime,fStartTime,fStartFlowRate,Quality);
  GetValue(HyFile,fVarFrom,fVarFrom,fEndTime,fEndTime,fEndFlowRate,Quality);
{ Concentration }
  if not Req(fIncVolume,0) then begin
    LoadUnits      := GlobalVariableList.Variable(VariableGetID(fVarTo)).VarUnit;
    VolumeUnits    := GlobalVariableList.Variable(VariableGetID(VolumeVarTo)).VarUnit;
    ConcUnits      := GlobalVariableList.Variable(VariableGetID(fEMC.fEMCVariable.EMCVar)).VarUnit;
    fConcentration := ConcUnits.SIToUnits(LoadUnits.UnitsToSI(fIncLoad)/VolumeUnits.UnitsToSI(fIncVolume));
  end;
end;

{------------------------------------------------------------------------------}
{                         TEMCPartitionList object                             }
{------------------------------------------------------------------------------}

constructor TEMCPartitionList.Create;
begin
  inherited Create;
  Sorted := True;
end;

{------------------------------------------------------------------------------}

function TEMCPartitionList.Compare(Item1,Item2: TObject): Integer;
begin
  Result := StrCompare(TEMCPartition(Item1).Key,TEMCPartition(Item2).Key,False);
end;

{------------------------------------------------------------------------------}

procedure TEMCPartitionList.Calculate(HyFile: THyFile);
var
  i: Integer;
begin
  for i := 0 to Count-1 do begin
    TEMCPartition(Items[i]).Calculate(HyFile);
  end;
end;

{------------------------------------------------------------------------------}
{                                TEMC object                                   }
{------------------------------------------------------------------------------}

constructor TEMC.Create(Event: TEvent; EMCVariable: TEMCVariable);
begin
  inherited Create;
  fEvent               := Event;
  fEMCVariable         := EMCVariable;
  fValue               := 0;
  fLoad                := 0;
  fVolume              := 0;
  fLoadQuality         := ConfigRec.DefBadQual;
  fVolumeQuality       := ConfigRec.DefBadQual;
  fNumSamples          := 0;
  fHeadVolumeScore     := 0;
  fInteriorVolumeScore := 0;
  fTailVolumeScore     := 0;
  fHeadLoadScore       := 0;
  fInteriorLoadScore   := 0;
  fTailLoadScore       := 0;
  fVolumeScore         := 0;
  fLoadScore           := 0;
  fSampleScore         := 0;
  fFlowRateScore       := 0;
  fScore               := 0;
  fUseSampleScore      := False;
  fUseVolumeScore      := False;
  fComposite           := False;
  fErrorCode           := 0;
  fPartitionList       := TEMCPartitionList.Create;
end;

{------------------------------------------------------------------------------}

destructor TEMC.Destroy;
begin
  fPartitionList.Free;
  inherited Destroy;
end;

{------------------------------------------------------------------------------}

function TEMC.GetFieldNames: String;
begin
  with fFieldNames do begin
    Clear;
    Add('ID');
    Add('EMC_VAR_NO');
    Add('Site');
    Add('Event_ID');
    Add('Parameter');
    Add('StartDate');
    Add('EndDate');
    Add('EMC');
    Add('EMC_Unit');
    Add('Total_Load');
    Add('Total_Load_Unit');
    Add('Total_Load_Qual');
    Add('Total_Volume');
    Add('Total_Volume_Unit');
    Add('Total_Volume_Qual');
    Add('Peak_Flowrate');
    Add('Samples');
    Add('Head_Volume_Score');
    Add('Interior_Volume_Score');
    Add('Tail_Volume_Score');
    Add('Head_Load_Score');
    Add('Interior_Load_Score');
    Add('Tail_Load_Score');
    Add('Flowrate_Score');
    Add('Volume_Score');
    Add('Sample_Score');
    Add('Load_Score');
    Add('Load_Score_Class');
    Add('Review_Score');
    Add('Use_Flowrate_Score');
    Add('Use_Volume_Score');
    Add('Use_Sample_Score');
    Add('Use_Load_Score');
    Add('Use_Review_Score');
    Add('Require_Review');
    Add('Score');
    Add('Comment');
    Add('Error_Code');
  end;
  Result := inherited GetFieldNames;
end;

{------------------------------------------------------------------------------}

function TEMC.GetFieldValues: String;
begin
  with fFieldValues do begin
    Clear;
    Add(IntToStr(fEvent.ID));
    Add(IntToStr(fEMCVariable.VariableID));
    Add(Quote(StIDToS(fEvent.evSite)));
    Add(Quote(fEvent.EventID));
    Add(Quote(fEMCVariable.VariableName));
    Add(RelToTemplateTimeStr(fEvent.StartOfFlow,DateFormat));
    Add(RelToTemplateTimeStr(fEvent.EndOfFlow,DateFormat));
    Add(RToS(fValue,10,6,fsFixed));
    Add(Quote(fEMCVariable.EMCUnits));
    Add(RToS(fLoad,10,5,fsFixed));
    Add(Quote(fEMCVariable.LoadUnits));
    Add(IntToStr(fLoadQuality));
    Add(RToS(fVolume,10,2,fsFixed));
    Add(Quote(fEMCVariable.VolumeUnits));
    Add(IntToStr(fVolumeQuality));
    Add(RToS(fMaxFlowRate,10,2,fsFixed));
    Add(IntToStr(fNumSamples));
    Add(RToS(fHeadVolumeScore,10,2,fsFixed));
    Add(RToS(fInteriorVolumeScore,10,2,fsFixed));
    Add(RToS(fTailVolumeScore,10,2,fsFixed));
    Add(RToS(fHeadLoadScore,10,2,fsFixed));
    Add(RToS(fInteriorLoadScore,10,2,fsFixed));
    Add(RToS(fTailLoadScore,10,2,fsFixed));
    Add(RToS(fFlowRateScore,10,2,fsFixed));
    Add(RToS(fVolumeScore,10,2,fsFixed));
    Add(RToS(fSampleScore,10,2,fsFixed));
    Add(RToS(fLoadScore,10,2,fsFixed));
    Add(Quote(iifStr(fLoadScore >= 50,'Pass','Fail')));
    Add('NULL');
    Add('True');
    Add(BToSTF(fUseVolumeScore));
    Add(BToSTF(fUseSampleScore));
    Add('False');
    Add('False');
    Add(BToSTF((fFlowRateScore < 70) and (fVolumeScore < 70) and
      (fSampleScore < 70) and (fLoadScore < 50)));
    Add(RToS(fScore,10,2,fsFixed));
    Add('NULL');
    Add(IntToStr(fErrorCode));
  end;
  Result := inherited GetFieldValues;
end;

{------------------------------------------------------------------------------}

function TEMC.GetFieldTypes: String;
begin
  with fFieldTypes do begin
    Clear;
    Add('ID INTEGER');
    Add('EMC_VAR_NO FLOAT');
    Add('Site VARCHAR');
    Add('Event_ID VARCHAR');
    Add('Parameter VARCHAR');
    Add('StartDate DATETIME');
    Add('EndDate DATETIME');
    Add('EMC FLOAT');
    Add('EMC_Unit VARCHAR');
    Add('Total_Load FLOAT');
    Add('Total_Load_Unit VARCHAR');
    Add('Total_Load_Qual INTEGER');
    Add('Total_Volume FLOAT');
    Add('Total_Volume_Unit VARCHAR');
    Add('Total_Volume_Qual INTEGER');
    Add('Peak_Flowrate FLOAT');
    Add('Samples INTEGER');
    Add('Head_Volume_Score FLOAT');
    Add('Interior_Volume_Score FLOAT');
    Add('Tail_Volume_Score FLOAT');
    Add('Head_Load_Score FLOAT');
    Add('Interior_Load_Score FLOAT');
    Add('Tail_Load_Score FLOAT');
    Add('Flowrate_Score FLOAT');
    Add('Volume_Score FLOAT');
    Add('Sample_Score FLOAT');
    Add('Load_Score FLOAT');
    Add('Load_Score_Class VARCHAR(10)'); { Cannot be full 255 chars, otherwise record size is > 2 Kb }
    Add('Review_Score FLOAT');
    Add('Use_Flowrate_Score LOGICAL');
    Add('Use_Volume_Score LOGICAL');
    Add('Use_Sample_Score LOGICAL');
    Add('Use_Load_Score LOGICAL');
    Add('Use_Review_Score LOGICAL');
    Add('Require_Review LOGICAL');
    Add('Score FLOAT');
    Add('Comment VARCHAR');
    Add('Error_Code INTEGER');
  end;
  Result := inherited GetFieldTypes;
end;

{------------------------------------------------------------------------------}

function TEMC.GetDisplayText: String;
begin
  Result := StrPadRight(StIDToS(fEvent.evSite),SiteIDLen,' ')+' '+
            StrPadRight(fEvent.EventID,9,' ')+' '+
            VariableAsString(fEMCVariable.LoadVar)+' '+
            IToS(fNumSamples,7)+' '+
            RToS(fHeadVolumeScore,8,2,fsFixed)+' '+
            RToS(fInteriorVolumeScore,8,2,fsFixed)+' '+
            RToS(fTailVolumeScore,8,2,fsFixed)+' '+
            RToS(fVolumeScore,8,2,fsFixed)+' '+
            RToS(fHeadLoadScore,8,2,fsFixed)+' '+
            RToS(fInteriorLoadScore,8,2,fsFixed)+' '+
            RToS(fTailLoadScore,8,2,fsFixed)+' '+
            RToS(fLoadScore,8,2,fsFixed)+' '+
            RToS(fSampleScore,8,2,fsFixed)+' '+
            RToS(fFlowRateScore,8,2,fsFixed)+' '+
            RToS(fScore,8,2,fsFixed);
end;

{------------------------------------------------------------------------------}

procedure TEMC.Calculate(HyFile: THyFile);
var
  i: Integer;
begin
{ Calculate total volume and load for event }
  GetValue(HyFile,FlowVarTo,VolumeVarTo,fEvent.StartOfFlow,fEvent.EndOfFlow,fVolume,fVolumeQuality);
  GetValue(HyFile,FlowVarTo,fEMCVariable.LoadVar,fEvent.StartOfFlow,fEvent.EndOfFlow,fLoad,fLoadQuality);

{ Calculate EMC for event - conversion factor }
  fValue := fLoad/(fVolume*fEMCVariable.Conversion);

{ Calculate EMC for event - variable conversion }
  //with HyFile.VarConList do begin
  //  VarConvert(fEMCVariable.LoadVar,fEMCVariable.EMCVar,fLoad,fValue,
  //    fEvent.StartOfFlow,fEvent.EndOfFlow-fEvent.StartOfFlow,fQuality,1);
  //  VarConvert(fEMCVariable.LoadVar,fEMCVariable.EMCVar,fLoad,fValue,
  //    fEvent.StartOfFlow,fEvent.EndOfFlow-fEvent.StartOfFlow,fQuality,2);
  //end;

{ Calculate EMC for event - unitcode }
  //EMCUnits    := GlobalVariableList.Variable(VariableGetID(fEMCVariable.EMCVar)).VarUnit;
  //LoadUnits   := GlobalVariableList.Variable(VariableGetID(fEMCVariable.LoadVar)).VarUnit;
  //VolumeUnits := GlobalVariableList.Variable(VariableGetID(VolumeVarTo)).VarUnit;
  //fValue      := EMCUnits.SIToUnits(LoadUnits.UnitsToSI(fLoad)/VolumeUnits.UnitsToSI(fVolume));

{ Calculate maximum flow rate for event }
  with HyFile do begin
    LoadHyValueVars(FlowVarTo,FlowVarTo);
    LoadHyValueTimes(fEvent.StartOfFlow,fEvent.EndOfFlow);
    Aggregate(False);
    fMaxFlowRate := HyValue.DMax;
  end;

{ Calculate partition volumes and loads }
  for i := 0 to PartitionList.Count-1 do begin
    TEMCPartition(PartitionList.Items[i]).Calculate(HyFile);
  end;
end;

{------------------------------------------------------------------------------}

procedure TEMC.ComputeScores(HyFile: THyFile; MedianVolume: TReal);
var
  FirstPartition,LastPartition,MaxVolumePartition,MaxLoadPartition: TEMCPartition;
  SampleFlowRate: TReal;
  MaxVolume,MaxLoad,Score,HeadScore: TReal;
  MaxVolumeIndex,MaxLoadIndex,i: Integer;
  USGSSite: Boolean;
begin
  if fNumSamples > 0 then begin
    Disp('Computing scores for event: '+fEvent.EventID);

  { Scan event partitions }
    MaxVolume      := 0;
    MaxLoad        := 0;
    MaxVolumeIndex := 0;
    MaxLoadIndex   := 0;
    SampleFlowRate := 0;
    for i := 0 to PartitionList.Count-1 do with TEMCPartition(PartitionList.Items[i]) do begin
      if (i <> 0) and (i <> PartitionList.Count-1) then begin { Ignore first and last partitions }

      { Find partion with greatest volume }
        if fIncVolume > MaxVolume then begin
          MaxVolume      := fIncVolume;
          MaxVolumeIndex := i;
        end;

      { Find partition with greatest load }
        if fIncLoad > MaxLoad then begin
          MaxLoad        := fIncLoad;
          MaxLoadIndex   := i;
        end;
      end;

    { Calculate maximum flow rate for samples }
      if fEndFlowRate > SampleFlowRate then SampleFlowRate := fEndFlowRate;
    end;

  { Save partitions of interest }
    FirstPartition     := TEMCPartition(PartitionList.Items[0]);
    LastPartition      := TEMCPartition(PartitionList.Items[PartitionList.Count-1]);
    MaxVolumePartition := TEMCPartition(PartitionList.Items[MaxVolumeIndex]);
    MaxLoadPartition   := TEMCPartition(PartitionList.Items[MaxLoadIndex]);

  { Is this a USGS site? }
    USGSSite := (StrLeft(StIDToS(HyFile.HSite),3) = '081') and fComposite;

  { Compute head volume score }
    if not Req(fVolume,0) then Score := FirstPartition.fIncVolume/fVolume else Score := 1;
    if fNumSamples = 1 then begin
      fHeadVolumeScore := iifReal(Score <= 1/3,120,150*(1-Score));
    end else begin
      fHeadVolumeScore := 120-2*Score*100;
    end;
    HeadScore := Score; { Save for tail volume score }

  { Compute interior volume score }
    if not USGSSite then begin
      if not Req(fVolume,0) then Score := MaxVolumePartition.fIncVolume/fVolume else Score := 1;
      if fNumSamples = 1 then begin
        fInteriorVolumeScore := 120;
      end else begin
        fInteriorVolumeScore := 120-Score*100;
      end;
    end else begin
      fInteriorVolumeScore := 100;
    end;

  { Compute tail volume score }
    if not Req(fVolume,0) then Score := LastPartition.fIncVolume/fVolume else Score := 1;
    if fNumSamples = 1 then begin
      fTailVolumeScore := iifReal(HeadScore <= 1/3,50+(150*HeadScore),120);
    end else begin
      fTailVolumeScore := 120-Score*100;
    end;

  { Compute overall volume score }
    Score := Min(Min(fHeadVolumeScore,fTailVolumeScore),fInteriorVolumeScore);
    if Score > 100 then Score := 100;
    fVolumeScore := Score;

  { Compute head load score }
    if not Req(fLoad,0) then Score := FirstPartition.fIncLoad/fLoad else Score := 1;
    if fNumSamples = 1 then begin
      fHeadLoadScore := fHeadVolumeScore;
    end else begin
      fHeadLoadScore := 120-2*Score*100;
    end;

  { Compute interior load score }
    if not USGSSite then begin
      if not Req(fLoad,0) then Score := MaxLoadPartition.fIncLoad/fLoad else Score := 1;
      if fNumSamples = 1 then begin
        fInteriorLoadScore := fInteriorVolumeScore;
      end else begin
        fInteriorLoadScore := 120-Score*100;
      end;
    end else begin
      fInteriorLoadScore := 100;
    end;

  { Compute tail load score }
    if not Req(fLoad,0) then Score := LastPartition.fIncLoad/fLoad else Score := 1;
    if fNumSamples = 1 then begin
      fTailLoadScore := fTailVolumeScore;
    end else begin
      fTailLoadScore := 120-Score*100;
    end;

  { Compute overall load score }
    Score := Min(Min(fHeadLoadScore,fTailLoadScore),fInteriorLoadScore);
    if Score > 100 then Score := 100;
    fLoadScore := Score;

  { Compute flow rate score }
    if not Req(fMaxFlowRate,0) and not USGSSite
      then fFlowRateScore := Sqrt(SampleFlowRate/fMaxFlowRate)*100
      else fFlowRateScore := 100;

  { Compute sample score }
    if not Req(fVolume,0) and not USGSSite
      then fSampleScore := 75+(10*(fNumSamples-((LogTen(fVolume/MedianVolume)/LogTen(2))+4)))
      else fSampleScore := 100;

  { Compute EMC score }
    if USGSSite then begin
      Score := fVolumeScore;
      fUseVolumeScore := True;
    end else if fNumSamples = 1 then begin
      Score := fSampleScore;
      fUseSampleScore := True;
    end else if fNumSamples = 2 then begin
      Score := fSampleScore;
      fUseSampleScore := True;
      if (fSampleScore >= 50) and (fVolumeScore > fSampleScore) then begin
        Score := fVolumeScore;
        fUseSampleScore := False;
        fUseVolumeScore := True;
      end;
    end else if fNumSamples = 3 then begin
      Score := fVolumeScore;
      fUseVolumeScore := True;
      if (fVolumeScore >= 50) and (fSampleScore > fVolumeScore) then begin
        Score := fSampleScore;
        fUseSampleScore := True;
        fUseVolumeScore := False;
      end;
    end else begin
      Score := fVolumeScore;
      fUseVolumeScore := True;
    end;
    if fFlowRateScore < Score then Score := fFlowRateScore;
    if Score < 0 then Score := 0;
    fScore := Score;

  { Assign error code }
    if Req(fVolume,0) then fErrorCode := 4                      { Zero volume }
    else if Req(fMaxFlowRate,0) then fErrorCode := 5            { Zero flow rate }
    else if Score >= 70 then fErrorCode := 0                    { Passing score }
    else if (Score < 70) and (Score >= 50) then fErrorCode := 1 { Marginal score }
    else if Score < 50 then fErrorCode := 2;                    { Failing score }

  end else begin
    fErrorCode := 3; { No samples }
  end;
end;

{------------------------------------------------------------------------------}
{                             TEMCList object                                  }
{------------------------------------------------------------------------------}

function TEMCList.Find(EMC: String): TEMC;
var
  i: Integer;
begin
  Result := nil;
  for i := 0 to Count-1 do begin
    if TEMC(Items[i]).fEMCVariable.VariableName = EMC then begin
      Result := TEMC(Items[i]);
      Break;
    end;
  end;
end;

{------------------------------------------------------------------------------}

procedure TEMCList.LoadPartitions(HyFile: THyFile; Event: TEvent);

  function IsSample(Comment: TStringList; var SampleID: String; var Composite: Integer): Boolean;
  var
    WordPeeler: TWordPeeler;
    Temp: String;
    i: Integer;
  begin
    Result    := False;
    SampleID  := '';
    Composite := 0;
    WordPeeler := TWordPeeler.Create('');
    try
      for i := 0 to Comment.Count-1 do begin
        WordPeeler.PeelString := Comment[i];
        while WordPeeler.NextPeelWord(Temp) do begin
          if Temp = 'ID' then begin
            WordPeeler.NextPeelWord(SampleID);
            Result := True;
          end else if Temp = 'CN' then begin
            WordPeeler.NextPeelWord(Temp);
            Composite := SToI(Temp);
          end;
        end;
      end;
    finally
      WordPeeler.Free;
    end;
  end;

var
  Comment: TStringList;
  FoundNO23,FoundTKN: Boolean;
  Variable: TVariable;
  EMCVariable: TEMCVariable;
  EMC: TEMC;
  StartPeriod,EndPeriod: TRelTime;
  Value: TReal;
  PartitionNum,Quality,Composite,i: Integer;
  SampleID: String;
const
  NO23Variable = 2333;
  TKNVariable  = 2336;
  TNVariable   = 2337;
begin
  Comment   := TStringList.Create;
  FoundNO23 := False;
  FoundTKN  := False;
  try
    for i := 0 to HyFile.VariableList.Count-1 do begin
      Variable := HyFile.VariableList.Variable[i];
      if not VariablesEQ(Variable,FlowVarTo) then begin
        if VariablesEQ(Variable,NO23Variable) then FoundNO23 := True
        else if VariablesEQ(Variable,TKNVariable) then FoundTKN := True;
        EMCVariable := EMCVariableList.Find(Variable);
        if Assigned(EMCVariable) then begin
          PartitionNum := 0;
          EMC := TEMC.Create(Event,EMCVariable);
          StartPeriod := Event.StartOfFlow-relSecond;
          HyFile.GetValue(Variable,StartPeriod,Value,Quality);
          HyFile.NextValue(Variable,StartPeriod,Value,Quality);

        { Was a sample taken at the start of the event? }
          HyFile.GetComments(Variable,Event.StartOfFlow,Comment);
          if IsSample(Comment,SampleID,Composite) and TimesEQ (StartPeriod,Event.StartOfFlow) then begin
            EMC.fComposite  := Composite > 1;
            EMC.fNumSamples := EMC.fNumSamples+1;
            PartitionNum    := PartitionNum+1;
            EMC.PartitionList.Add(TEMCPartition.Create(EMC,PartitionNum,SampleID,Composite,
              FlowVarTo,EMCVariable.LoadVar,StartPeriod,StartPeriod));
          end;

        { Create a partition for each sample }
          HyFile.NextValue(Variable,EndPeriod,Value,Quality);
          HyFile.GetComments(Variable,EndPeriod,Comment);
          while EndPeriod <= Event.EndOfFlow do begin
            if IsSample(Comment,SampleID,Composite) then begin
              EMC.fComposite  := Composite > 1;
              EMC.fNumSamples := EMC.fNumSamples+1;
            end else begin
              SampleID := 'End of event';
            end;
            PartitionNum := PartitionNum+1;
            EMC.PartitionList.Add(TEMCPartition.Create(EMC,PartitionNum,SampleID,Composite,
              FlowVarTo,EMCVariable.LoadVar,StartPeriod,EndPeriod));
            StartPeriod := EndPeriod;
            HyFile.NextValue(Variable,EndPeriod,Value,Quality);
            HyFile.GetComments(Variable,EndPeriod,Comment);
          end;

        { Was a sample taken at the end of the event? }
          HyFile.GetComments(Variable,Event.EndOfFlow,Comment);
          if IsSample(Comment,SampleID,Composite) and TimesEQ (StartPeriod,Event.EndOfFlow) then begin
            EMC.fComposite := Composite > 1;
            PartitionNum   := PartitionNum+1;
            EMC.PartitionList.Add(TEMCPartition.Create(EMC,PartitionNum,SampleID,Composite,
              FlowVarTo,EMCVariable.LoadVar,StartPeriod,StartPeriod));
          end;

          if EMC.fNumSamples > 0 then Add(EMC);
        end;
      end;
    end;

  { Create EMC for total nitrogen }
    if FoundNO23 and FoundTKN then begin
      EMCVariable := EMCVariableList.Find(TNVariable);
      if Assigned(EMCVariable) then begin
        EMC := TEMC.Create(Event,EMCVariable);
        Add(EMC);
      end;
    end;
  finally
    Comment.Free;
  end;
end;

{------------------------------------------------------------------------------}

procedure TEMCList.Calculate(HyFile: THyFile; MedianVolume: TReal);
var
  NO23,TKN,TN: TEMC;
  i: Integer;
begin
  for i := 0 to Count-1 do with TEMC(Items[i]) do begin
    Calculate(HyFile);
    ComputeScores(HyFile,MedianVolume);
  end;

{ Compute total nitrogen }
  NO23 := Find('NO23');
  TKN  := Find('TKN');
  TN   := Find('TN');
  if Assigned(NO23) and Assigned(TKN) and Assigned(TN) then begin
    TN.fValue               := NO23.fValue + TKN.fValue;
    TN.fLoad                := NO23.fLoad + TKN.fLoad;
    TN.fVolume              := NO23.fVolume + TKN.fVolume;
    TN.fMaxFlowRate         := Max(NO23.fMaxFlowRate,TKN.fMaxFlowRate);
    TN.fLoadQuality         := Max(NO23.fLoadQuality,TKN.fLoadQuality);
    TN.fVolumeQuality       := Max(NO23.fVolumeQuality,TKN.fVolumeQuality);
    TN.fNumSamples          := Min(NO23.fNumSamples,TKN.fNumSamples);
    TN.fHeadVolumeScore     := Min(NO23.fHeadVolumeScore,TKN.fHeadVolumeScore);
    TN.fInteriorVolumeScore := Min(NO23.fInteriorVolumeScore,TKN.fInteriorVolumeScore);
    TN.fTailVolumeScore     := Min(NO23.fTailVolumeScore,TKN.fTailVolumeScore);
    TN.fHeadLoadScore       := Min(NO23.fHeadLoadScore,TKN.fHeadLoadScore);
    TN.fInteriorLoadScore   := Min(NO23.fInteriorLoadScore,TKN.fInteriorLoadScore);
    TN.fTailLoadScore       := Min(NO23.fTailLoadScore,TKN.fTailLoadScore);
    TN.fVolumeScore         := Min(NO23.fVolumeScore,TKN.fVolumeScore);
    TN.fLoadScore           := Min(NO23.fLoadScore,TKN.fLoadScore);
    TN.fSampleScore         := Min(NO23.fSampleScore,TKN.fSampleScore);
    TN.fFlowRateScore       := Min(NO23.fFlowRateScore,TKN.fFlowRateScore);
    TN.fScore               := Min(NO23.fScore,TKN.fScore);
    TN.fUseSampleScore      := NO23.fUseSampleScore or TKN.fUseSampleScore;
    TN.fUseVolumeScore      := NO23.fUseVolumeScore or TKN.fUseVolumeScore;
    TN.fErrorCode           := Max(NO23.fErrorCode,TKN.fErrorCode);
  end;
end;

{------------------------------------------------------------------------------}

procedure TEMCList.WriteToList;
var
  PrevEventID: String;
  i{,j}: Integer;
begin
  PrevEventID := '';
  for i := 0 to Count-1 do with TEMC(Items[i]) do begin
    if fEvent.EventID <> PrevEventID then ListDev.Writeln;
    ListDev.Writeln(DisplayText);
    PrevEventID := fEvent.EventID;
    //for j := 0 to PartitionList.Count-1 do with TEMCPartition(PartitionList.Items[j]) do begin
    //  ListDev.Writeln(DisplayText);
    //end;
    //ListDev.Writeln;
  end;
end;

{------------------------------------------------------------------------------}
{                          TBaseFlowParams object                              }
{------------------------------------------------------------------------------}

constructor TBaseFlowParams.Create(Site: TStID; Variable: TVariable;
  Value: TReal; Quality: Integer; Positive: Boolean);
begin
  inherited Create;
  fSite     := Site;
  fVariable := Variable;
  fValue    := Value;
  fQuality  := Quality;
  fPositive := Positive;
end;

{------------------------------------------------------------------------------}
{                        TBaseFlowParamsList object                            }
{------------------------------------------------------------------------------}

procedure TBaseFlowParamsList.LoadRecords(DB: TADODB; TableName: String);
begin
  with DB.ADOQuery do begin
    DB.AddSQL('SELECT * FROM '+TableName);
    Open;
    First;
    while not EOF do begin
      Add(TBaseFlowParams.Create(
        SToStID(FieldByName('Site').AsString),
        VariableCompose(FieldByName('Varnum').AsString),
        FieldByName('Value').AsFloat,
        FieldByName('Quality').AsInteger,
        FieldByName('Sign').AsString = '+'));
      Next;
    end;
    Close;
  end;
end;

{------------------------------------------------------------------------------}

function TBaseFlowParamsList.Find(aSite: TStID; aVariable: TVariable): TBaseFlowParams;
var
  BaseFlowParams: TBaseFlowParams;
  i: Integer;
begin
  Result := nil;
  for i := 0 to Count-1 do begin
    BaseFlowParams := TBaseFlowParams(Items[i]);
    if (StIDCompare(aSite,BaseFlowParams.fSite) = 0) and
        VariablesEQ(aVariable,BaseFlowParams.fVariable) then begin
      Result := BaseFlowParams;
      Break;
    end;
  end;
end;

{------------------------------------------------------------------------------}

begin
  StartUpProcPtr  := StartUp;
  DoMainProcPtr   := DoMain;
  ShutDownProcPtr := ShutDown;
  ProgName         := PrgName;
end.

