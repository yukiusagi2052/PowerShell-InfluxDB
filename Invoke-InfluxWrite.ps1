<#
  Title    : Invoke-InfluxWrite
  Version  : 0.21
  Updated  : 2015/5/10

  Tested   : InfluxDB 0.8.8
             Powershell 4.0
#>

## 設定パラメーター ##

  # 例外処理発生時のデフォルト動作を定義
    $ErrorActioPreference = "Stop" #停止
    #$ErrorActioPreference = "Inquire" #ユーザー問い合わせ

  # InfluxDB ネットワーク・パラメータ
    [String]$DefaltInfluxServer   = 'localhost'
    [String]$DefaltInfluxPort     = '8086'
    [String]$DefaltInfluxDbName   = 'metrics'
    [String]$DefaltInfluxUsername = 'admin'
    [String]$DefaltInfluxPassword = 'pass'

  # Invoke-HttpMethod パラメータ
    [Int] $MethodRetryWaitSecond = 10
    [Int] $MaxMethodRetry = 6

  # Invoke-InfluxWrite パラメータ
    [Int]$CounterShowThreshold = 20
    [Int]$WriteIntervalCount = 100

  # Unix Epoch Time 基準
    [datetime]$EpochOrigin        = "1970/01/01 09:00:00"


Function Check-PSVersionCompatible{
  Param(
    [int]$RequireVersion
  )
  #互換性の有無
  $Compatible = $False

  ForEach ($v in $PSVersionTable.PSCompatibleVersions){
    If ( $v.Major -eq $RequireVersion){
      $Compatible = $True
    }
  }
  Write-Output $Compatible
}

Function Get-NowEpoch {
  <#
  .SYNOPSIS
    現在のUNIX Epoch Time（単位ミリ秒）を取得
  #>
  (New-TimeSpan -Start (Get-Date $EpochOrigin) -End ([DateTime]::Now)).TotalMilliseconds
}

Function ConvertTo-UnixEpoch {
  <#
  .SYNOPSIS
   「日時文字列（YYYY/MM/DD HH:MM:SS）」→ 「UNIX Epoch Time（単位ミリ秒）」変換
  #>
  Param([string]$DateTime)
  (New-TimeSpan -Start (Get-Date $EpochOrigin) -End (Get-Date $DateTime)).TotalMilliseconds
}

function Invoke-HttpMethod {
  [CmdletBinding()]
  Param(
      [string] $URI,
      [string] $Method,
      [string] $Body
  )


  #チェックフラグ
  [Bool] $MethodResult = $True
  
  For($WriteRetryCount=0; $WriteRetryCount -lt $MaxMethodRetry; $WriteRetryCount++){

    $WebRequest = [System.Net.WebRequest]::Create($URI)
    $WebRequest.ContentType = "application/x-www-form-urlencoded"
    $BodyStr = [System.Text.Encoding]::UTF8.GetBytes($Body)
    $Webrequest.ContentLength = $BodyStr.Length
    $WebRequest.ServicePoint.Expect100Continue = $false
    $webRequest.Method = $Method


    # [System.Net.WebRequest]::GetRequestStream()
    Try{
      $RequestStream = $WebRequest.GetRequestStream()

      # [System.IO.Stream]::Write()
      Try{
        $RequestStream.Write($BodyStr, 0, $BodyStr.length)
      } Catch [Exception] {
        Write-Error $Error[0].Exception.ErrorRecord
        $MethodResult = $False
      }
      $MethodResult = $True

    } Catch [Exception] {
      Write-Error $Error[0].Exception.ErrorRecord
      $MethodResult = $False
    } Finally {
      $RequestStream.Close()
    }

    # [System.Net.WebRequest]::GetResponse()
    If($MethodResult){
      Try{
        [System.Net.WebResponse] $resp = $WebRequest.GetResponse();
        $MethodResult = $True
      } Catch [Exception] {
        Write-Error $Error[0].Exception.ErrorRecord
        $MethodResult = $False
      }
    }

    # [System.Net.WebResponse]::GetResponseStream()
    If($MethodResult){
      Try{
        $rs = $resp.GetResponseStream();
        $MethodResult = $True
      } Catch [Exception] {
        Write-Error $Error[0].Exception.ErrorRecord
        $MethodResult = $False
      }
    }

    # [System.IO.StreamReader]::ReadToEnd()
    If($MethodResult){
      Try{
        [System.IO.StreamReader] $sr = New-Object System.IO.StreamReader -argumentList $rs;
        [string] $results = $sr.ReadToEnd();
        $MethodResult = $True
      } Catch [Exception] {
        Write-Error $Error[0].Exception.ErrorRecord
        $MethodResult = $False
      } Finally {
        $sr.Close();
      }
    }

    If($MethodResult){
      #最終的にMethod成功
        return $results;
    } Else {
      #最終的にMethod失敗
      If ($WriteRetryCount -lt $MaxMethodRetry) {
        #リトライの準備
        Write-Verbose "DB書き込みをリトライします"
        Remove-Variable RequestStream
        Remove-Variable BodyStr
        Remove-Variable WebRequest
        #[System.GC]::Collect([System.GC]::MaxGeneration)
        Start-Sleep -Seconds $MethodRetryWaitSecond
      } Else {
        #最大リトライ回数に到達
        Write-Verbose "最大リトライ回数に達しました。スキップします"
      }
    }
  } #For .. (WriteRetry) .. Loop
}

Function Invoke-InfluxWriteRaw {
  <#
  .SYNOPSIS
    低レベル InfluxDB 書き込み関数
  #>
  [CmdletBinding()]
  Param(
    [string]$Server,
    [string]$Port,
    [string]$DbName,
    [string]$Username,
    [string]$Password,
    [string]$Series,
    [string]$Columns,
    [string]$Points
  )
    #InfluxDB APIアクセス用 URI作成
    [String]$resource = "http://" + $Server + ":" + $Port + "/db/" + $DbName `
                          + "/series?u=" + $Username + "&p=" + $Password

    #InfluxDB APIアクセス用 POSTデータ(JSON文字列)の全体を作成
    $body = '[{"name":"' + $Series + '","columns":[' + $Columns + '],"points":[' + $Points + ']}]'

    #デバッグ用
    Write-Debug "Post URI : $resource"
    Write-Debug "Post Data: $body"
    
    #InfluxDB APIアクセス(Post実行)
    Invoke-HttpMethod -Uri $resource -Method POST -Body $body -Debug
    
    <#
    If ($PSVersionTable.PSVersion.Major -ge 3){
      # Powershell 3.0以上
      Invoke-RestMethod -Uri $resource -Method POST -Body $body
    } ElseIF ($PSVersionTable.PSVersion.Major -eq 2) {
      # Powershell 2.0 (Windows7, Windows 2008R2)
      Invoke-HttpMethod -Uri $resource -Method POST -Body $body
    } ElseIF ($PSVersionTable.PSVersion.Major -eq 1) {
      # Powershell 1.0 (Vista, Windows2003)
      Write-Host "Powershellバージョンが不適合です。（バージョン 2.0以上が必要です）"
      return
    }
    #>
}

Function Invoke-InfluxWrite {
  <#
  .SYNOPSIS
    Writing Data into InfluxDB
  .DESCRIPTION
    This Function write a piped object(like mesured data) into influxDB.
    You can write a single object or multiple objects. 
  .EXAMPLE
    Get-WmiObject Win32_Logicaldisk | Where-Object DeviceID -eq "C:" | Select-Object FreeSpace | Invoke-InfluxWrite -Series server1.freedisk.c
  .EXAMPLE
    Import-Csv -Path C:\Data.csv | Invoke-InfluxWrite

  .PARAMETER SeriesName
    InfluxDB Series Name. (Series like a database's Table)
  .PARAMETER TagColumns
    InfluxDB Columns as Tags. Tag is not used data.
  .PARAMETER Verbose
    Display Writing progress

  .PARAMETER Server
    InfluxDB Server FQDN or IP Address
  .PARAMETER Port
    InfluxDB REST api Access Port Number 
  .PARAMETER DbName
    InfluxDB Database Name 
  .PARAMETER Username
    InfluxDB Access Username
  .PARAMETER Password
    InfluxDB Access Password 
    
  #>
  Param(
    [string]$Server   = $DefaltInfluxServer,
    [string]$Port     = $DefaltInfluxPort,
    [string]$DbName   = $DefaltInfluxDbName,
    [string]$Username = $DefaltInfluxUsername,
    [string]$Password = $DefaltInfluxPassword,
    [string]$SeriesName,
    [string[]]$TagColumns,
    [switch]$Verbose
  )

  BEGIN{
    #必要なPowerShell バージョンであるか確認
    If( !(Check-PSVersionCompatible 2) ){
      Write-Error "Powershell (Version 2.0 up) is needed Invoke-InfluxWrite"
      return
    }

    #PROCESSブロック内の初期化したか、確認フラグ
    [bool]$ProcessBlockInit = $false

    #プロパティ「series」を含むか、確認フラグ
    [bool]$ContainSeries = $false

    # Invoke-InfluxWriteRaw -columns に含める対象のプロパティを
    #「（Key)プロパティ名：(値) 文字列 / 非文字列 」でカタログする。
    $ColumnsCatalog = @{}

    #InfluxDB APIアクセス用 POSTデータ(JSON文字列)
    [String]$ColumnsStrBuffer = ""
    [String]$SinglePointsStrBuffer = ""
    [String[]]$MultiPointsArrayBuffer = @()

    #進捗表示 カウンター
	[int]$ProgressCounter = 0

    #データの「PSObject ⇒ JSON文字列」変換の成功・失敗フラグ
    [Bool] $ObjectToJsonConvertResult = $false
	}


  PROCESS {
    ##
    ## 初期化処理
    ## パイプで渡されたオブジェクトの1個目を解析して、
    ## 後続処理で使う $ColumnsCatalog を作成する。
    ##
    If( !$ProcessBlockInit ) {

      #パイプでオブジェクトが渡されたかチェック
      If( $_ -eq $Null ){
        Write-Error "There is no piped object."
        return
      }

      #初期化処理内ローカル変数宣言
      [Object]$Obj = $_
      [String[]]$ColumnNames = @()

      $Props = $Obj | Get-Member -MemberType NoteProperty

      # 対象オブジェクトの「各プロパティ」を精査開始
      ForEach ($P in $Props) {

        # デバッグ
        # $P.Name

        Switch($P.Name) {
          "series" {
            #プロパティ「series」は、Invoke-InfluxWriteRaw の -Points に含む対象でないため
            #$ColumnsCatalogへ追加しない。

            #さらに、後続処理のため判定フラグを立てる。
            $ContainSeries = $true
          }

          "time"　{
            #プロパティ「time」は、Invoke-InfluxWriteRaw の -Points に含む対象のため、
            #$ColumnsCatalogへ追加する
            $ColumnsCatalog.Add("time", "NotString" )
          }
          
          "sequence_number"　{
            #プロパティ「sequence_number」は、Invoke-InfluxWriteRaw の -Points に含む対象のため、
            #$ColumnsCatalogへ追加する
            $ColumnsCatalog.Add("sequence_number", "NotString" )
          }
          
          default{
            # "series", "time", "sequence_number"以外のプロパティの場合

            If ( $TagColumns -ne $Null ){
              # Invoke-InfluxWrite -TagColumns $TagColumns の指定有り

              # -TagColumnsで指定されたプロパティは、InfluxDBへ書き込むときに文字列型として
              # 処理されるように、$ColumnsCatalogに登録する
              If ( $TagColumns -contains ($P.Name) ){
                $ColumnsCatalog.Add( $P.Name , "String" ) #文字列型を指定
              }Else{
                $ColumnsCatalog.Add( $P.Name , "NotString" ) #非文字列型を指定
              }

            } Else {
              # Invoke-InfluxWrite -TagColumns $TagColumns の指定無し
                $ColumnsCatalog.Add( $P.Name , "NotString" ) #非文字列型を指定
            }

          }

        }
      }# 対象オブジェクトの「各プロパティ」を精査完了

      #
      # InfluxDB APIアクセス用 POSTデータ(JSON文字列)の一部(Columns)を作成
      #
      # Invoke-InfluxWriteRaw -Columns $ColumnsStrBuffer
      #
      ForEach ($CatalogKey in $ColumnsCatalog.Keys){
        # 一時変数 $ColumnNames へ追加
        $ColumnNames += ('"' + $CatalogKey + '"')
      }
      $ColumnsStrBuffer = ($ColumnNames -join ",")


      #初期化処理内ローカル変数を開放
      Remove-Variable ColumnNames
      Remove-Variable Obj

      #初期化済みフラグを立てる
      $ProcessBlockInit = $true

    } #初期化完了

    ##
    ## pipeされた各オブジェクトを処理するメイン処理
    ##

    # オブジェクト処理回数カウンター
    # $CounterShowThreshold 回以上になったら、進捗表示する
    if($ProgressCounter -gt $CounterShowThreshold){
      If($Verbose){ Write-host -nonewline "`b`b`b`b`b`b`b`b`b`b`b`b$ProgressCounter" }
    }
  $ProgressCounter++

    #ローカル変数宣言
    [String[]]$TempPointsString = @()


    $ObjectToJsonConvertResult = $True
    Try{
      # パイプで受けたオブジェクトの各プロパティから、Invoke-InfluxWriteRaw -columns に含める
      # 対象となるものを選別して、一時変数 $TempPointsString へ格納して、後続処理に回す。
      #
      #   $ColumnsCatalog の Key        --  対象Object の プロパティName
      #   $CatalogKey                   --  同上
      #   $ColumnsCatalog[$CatalogKey]  --  文字列/非文字列
      #   $_.対象プロパティのName       --  パイプされたObjectの値
      #
      ForEach ($CatalogKey in $ColumnsCatalog.Keys){
        If ($CatalogKey -eq "time" ){
          If( ($_.$CatalogKey) -eq $Null ){
            # プロパティ「time」の値が空の場合
            # 現在の Unix Epochを、一時変数 $TempPointsStrin へ格納
            $TempPointsString += [string](Get-NowEpoch)
          } Else {
            # プロパティ「time」の値が空ではない場合
            # Unix Epochに変換し、一時変数 $TempPointsStrin へ格納
            $TempPointsString += [string](ConvertTo-UnixEpoch ($_.$CatalogKey) )
          }
        } Else {
          If( $ColumnsCatalog[$CatalogKey] -eq "String" ) {
            # プロパティ「time」以外で、文字列の場合
            # ダブルクォートで囲んで、一時変数 $TempPointsStrin へ格納
            $TempPointsString += ('"' + [string]($_.$CatalogKey) + '"')
          }Else{
            # プロパティ「time」以外で、非文字列の場合
            # 素のまま、一時変数 $TempPointsStrin へ格納
            $TempPointsString += [string]($_.$CatalogKey)
          }
        }

      }# ForEache Loop --> |

    } Catch [Exception] {
      Write-Error $Error[0].Exception.ErrorRecord
      $ObjectToJsonConvertResult = $False
    }


    #Series名をパイプで渡されたかで、処理分岐
    If( $ObjectToJsonConvertResult -and $ContainSeries ) {
      #seriesをパイプで受けたオブジェクトに含む場合は、InfluxDBへ書込み実行
      $SinglePointsStrBuffer =  ( '[' + ( $TempPointsString -join "," ) + ']' )

      #seriesに値が含まれるかチェックしてから、DB書き込み
      IF ($_.series -ne $Null) {
        #オブジェクトのプロパティ「series」に値が含まれている場合
        If ($Verbose){
          Invoke-InfluxWriteRaw -Verbose -Server $Server -Port $Port -DbName $DbName -Username $Username -Password $Password -Series $_.series -Columns $ColumnsStrBuffer -Points $SinglePointsStrBuffer > $null
        } Else {
          Invoke-InfluxWriteRaw -Server $Server -Port $Port -DbName $DbName -Username $Username -Password $Password -Series $_.series -Columns $ColumnsStrBuffer -Points $SinglePointsStrBuffer > $null
        }
      } ElseIF ( ($_.series -eq $Null) -and ($SeriesName -ne $Null) ) {
        #オブジェクトのプロパティ「series」に値がなく、関数パラメータで-SeriesNameが指定されている場合
        If ($Verbose){
          Invoke-InfluxWriteRaw -Verbose -Server $Server -Port $Port -DbName $DbName -Username $Username -Password $Password -Series $SeriesName -Columns $ColumnsStrBuffer -Points $SinglePointsStrBuffer > $null
        } Else {
          Invoke-InfluxWriteRaw -Server $Server -Port $Port -DbName $DbName -Username $Username -Password $Password -Series $SeriesName -Columns $ColumnsStrBuffer -Points $SinglePointsStrBuffer > $null
        }
      } Else {
        #オブジェクトのプロパティ「series」に値がなく、関数パラメータで-SeriesNameもない場合
        #書き込み不可
        Write-Error "There is no series name. $SinglePointsStrBuffer"
      }
    } ElseIf ( $ObjectToJsonConvertResult -and !$ContainSeries) {
      # seriesをパイプで受けたオブジェクトに含まない場合は
      # オブジェクトを一定個数、書き込みバッファに蓄積し、InfluxDBへバッチ書き込みを実行
      $MultiPointsArrayBuffer += ( '[' + ( $TempPointsString -join "," ) + ']' )
      
      # オブジェクトを一定個数処理したかチェック
      if ( ($ProgressCounter % $WriteIntervalCount) -eq 0 ) {
        # 書き込みバッファーを、書き込み
        If ($Verbose){
          Invoke-InfluxWriteRaw -Verbose -Server $Server -Port $Port -DbName $DbName -Username $Username -Password $Password -Series $SeriesName -Columns $ColumnsStrBuffer -Points ( $MultiPointsArrayBuffer -join "," ) > $null
        } Else {
          Invoke-InfluxWriteRaw -Server $Server -Port $Port -DbName $DbName -Username $Username -Password $Password -Series $SeriesName -Columns $ColumnsStrBuffer -Points ( $MultiPointsArrayBuffer -join "," ) > $null
        }
        # 書き込みバッファをクリア
        $MultiPointsArrayBuffer = @()
        # 詳細表示用
        If($Verbose){ Write-Host "`b`b`b`b`b`b`b`b`b`b`b`b$ProgressCounter post -> InfluxDB" }
      }
    }

  } #PROCESS ここまで

  END{
    ##Series名をパイプで渡されたかで、処理分岐
    If ( $ContainSeries ) {
      # seriesをパイプで受けたオブジェクトに含む場合は、各オブジェクト単位でInfluxDBへ書き込み済み
    } Else {

      # 詳細表示用
      if($ProgressCounter -gt $CounterShowThreshold){
        If($Verbose){ Write-Host "`b`b`b`b`b`b`b`b`b`b`b`b$ProgressCounter post -> InfluxDB" }
      }

      # seriesをパイプで受けたオブジェクトに含まない場合は
      # 書き込みバッファの残りをすべて、InfluxDBへバッチ書き込みを実行
      If ($Verbose){
        Invoke-InfluxWriteRaw -Verbose -Server $Server -Port $Port -DbName $DbName -Username $Username -Password $Password -Series $SeriesName -Columns $ColumnsStrBuffer -Points ( $MultiPointsArrayBuffer -join "," ) > $null
      } Else {
        Invoke-InfluxWriteRaw -Server $Server -Port $Port -DbName $DbName -Username $Username -Password $Password -Series $SeriesName -Columns $ColumnsStrBuffer -Points ( $MultiPointsArrayBuffer -join "," ) > $null
      }
    }
  }

}
