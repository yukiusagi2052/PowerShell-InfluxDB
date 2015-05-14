<#
  Title    : Invoke-InfluxQuery.ps1
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

Function ConvertTo-dotNetDateTime {
  <#
  .SYNOPSIS
   「UNIX Epoch Time（単位ミリ秒）」→「日時文字列（YYYY/MM/DD HH:MM:SS）」変換 
  #>
  Param([string]$UnixEpoch)
  $EpochOrigin.AddMilliSeconds($UnixEpoch)
}

Function Invoke-InfluxQuery {
  <#
  .SYNOPSIS
    Query to InfluxDB
  .DESCRIPTION
    This Function Query to influxDB and retuen result as PSObjects.
  .EXAMPLE
    Invoke-InfluxQuery 'list series'
  .EXAMPLE
    Invoke-InfluxQuery "select * from server1.freedisk.c"
  .PARAMETER Query
    InfluxDB Query String. See also InfluxDB Documents.
    http://influxdb.com/docs/v0.8/api/query_language.html
  #>
  [CmdletBinding()] 
  param(
    [Parameter(Mandatory=$True, Position=1,
      ValueFromPipeline=$True, ValueFromPipelineByPropertyName=$True,
      HelpMessage='What Query will you invoke to InfluxDB')]
    [string]$Query,
    [string]$Server   = $DefaltInfluxServer,
    [string]$Port     = $DefaltInfluxPort,
    [string]$DbName   = $DefaltInfluxDbName,
    [string]$Username = $DefaltInfluxUsername,
    [string]$Password = $DefaltInfluxPassword
  )

  #必要なPowerShell バージョンであるか確認
  If( !(Check-PSVersionCompatible 3) ){
    Write-Host "Powershellバージョンが不適合です。（バージョン 3.0以上 必須）"
    return
  }

  #InfluxDB APIアクセス用 URI作成
  [String]$resource = "http://" + $Server + ":" + $Port + "/db/" + $DbName `
                          + "/series?" + "q=" + $Query + "&u=" + $Username + "&p=" + $Password # + "&pretty=true"

  #デバッグ用
  Write-Verbose "Query URI: $resource"
    
  #InfluxDB APIアクセス(Post実行)
  Try {
    $Res = Invoke-RestMethod -Uri $resource -Method GET
  } Catch [Exception] {
    Write-host $Error[0].Exception.ErrorRecord
  }

  #REST API Responseが空でない場合は、PowerShell オブジェクトに変換
  If ( ($Res -ne $null) -and ($Res.points -ne $null) ) {


    # Getした値(Points)が、マルチかシングルか
    If ($Res.points[0] -is [system.array]){
    #Case Multi-Points (マルチの場合)

      [Object[]]$ObjArray = @()

      For ($row=0; $row -lt $Res.points.Length; $row++){

        [Object]$Obj = New-Object PSObject
        $Obj | Add-Member –MemberType NoteProperty –Name "series" –Value $Res.name

        For ($col=0; $col -lt $Res.columns.Length; $col++){
          If ( ($Res.columns[$col] -eq "time") -and ($Res.points[$row][$col] -ne "0") ){
            #timeで、かつtime値が0以外のときは、時刻型に変換し、追加
            $Obj | Add-Member –MemberType NoteProperty –Name $Res.columns[$col] –Value (ConvertTo-dotNetDateTime $Res.points[$row][$col])
          } ElseIf( ($Res.columns[$col] -eq "time") -and ($Res.points[$row][$col] -eq "0") ) {
            #値を捨てる
          } Else {
            #time 以外のときは、そのまま追加
            $Obj | Add-Member –MemberType NoteProperty –Name $Res.columns[$col] –Value $Res.points[$row][$col]
          }      
        }#columns loop#

        $ObjArray += $Obj
        Remove-Variable Obj
      }#rows loop#

      Write-Output $ObjArray
      Remove-Variable ObjArray

    } Else {
    # Case Single-Point (シングルの場合)

        [Object]$Obj = New-Object PSObject

        $Obj | Add-Member –MemberType NoteProperty –Name "series" –Value $Res.name

        for ($col=0; $col -lt $Res.columns.Length; $col++){
          If ( ($Res.columns[$col] -eq "time") -and ($Res.points[$col] -ne "0") ){
            #timeで、かつtime値が0以外のときは、時刻型に変換し、追加
            $Obj | Add-Member –MemberType NoteProperty –Name $Res.columns[$col] –Value (ConvertTo-dotNetDateTime $Res.points[$col])
          } ElseIf( ($Res.columns[$col] -eq "time") -and ($Res.points[$col] -eq "0") ) {
            #値を捨てる
          } Else {
            #time 以外のときは、そのまま追加
            $Obj | Add-Member –MemberType NoteProperty –Name $Res.columns[$col] –Value $Res.points[$col]
          }      
        }#columns loop#

        Write-Output $Obj
        Remove-Variable Obj
    }
  }
}

