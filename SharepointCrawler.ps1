$targets= @("the dreaded sharepoint site")

Connect-MGGraph -ClientId "a" `
                -TenantId "b" `
                -CertificateThumbprint "c" `
                -ContextScope "Process"

$syncVar = [HashTable]::Synchronized(@{})
$syncVar.sites = [System.Collections.Hashtable]::Synchronized([System.Collections.Hashtable]::new())
$syncVar.drives = [System.Collections.Hashtable]::Synchronized([System.Collections.Hashtable]::new())
$syncVar.folders = [System.Collections.Hashtable]::Synchronized([System.Collections.Hashtable]::new())
$syncVar.files = [System.Collections.Hashtable]::Synchronized([System.Collections.Hashtable]::new())
$syncVar.versions = [System.Collections.Hashtable]::Synchronized([System.Collections.Hashtable]::new())
$syncVar.siteQueue = [System.Collections.Concurrent.ConcurrentQueue[psobject]]::new()
$syncVar.folderQueue = [System.Collections.Concurrent.ConcurrentQueue[psobject]]::new()
$syncVar.fileQueue = [System.Collections.Concurrent.ConcurrentQueue[psobject]]::new()
$syncVar.activeSites = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]::new())
$syncVar.activeDrives = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]::new())
$syncVar.activeFolders = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]::new())
$syncVar.activeFiles = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]::new())
$syncVar.targets = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]::new())

$siteThreads = 1
$driveThreads = 1
$folderThreads = 4
$fileThreads = 8

$throttleLimit = $siteThreads + $driveThreads +$folderThreads + $fileThreads

######################
# SCRIPTBLOCKS START #
######################
$getSharepointSitesThread = {
    param($syncVar)
    function FeedResultToSyncVar
    {
        param($response)   
       
        for ($i = 0;$i -lt $response.value.Count; $i++)
        {
            
            if ($response.value[$i].weburl -notmatch "-my.sharepoint.com")
            {
                $syncVar.sites[($response.value[$i].id)] = $response.value[$i]
                #$syncVar.siteQueue.Enqueue($response.value[$i].id)
                if ($syncVar.targets.count -gt 0 -and $response.value[$i].name -in $syncvar.targets)
                {
                    $syncVar.siteQueue.Enqueue($response.value[$i].id)
                }
                
            }
            
        }
    }
    $response = Invoke-MgGraphRequest -Method GET -Uri "https://graph.microsoft.com/beta/sites"
    FeedResultToSyncVar -response $response
    while ($response.'@odata.nextLink')
    {
        $response = Invoke-MgGraphRequest -Method GET -Uri $response.'@odata.nextLink'
        FeedResultToSyncVar -response $response
    }
}



$getSharepointDrivesThread = {
    param($syncVar,$poolIndex)
    function FeedResultToSyncVar
    {
        param($response)   
        $values = @($response.value)
        for ($i = 0;$i -lt $values.Count; $i++)
        {
            $syncVar.drives[($response.value[$i].id)] = $response.value[$i]
            if ($response.value[$i].quota.used -gt 0)
            {
                $syncVar.folderQueue.Enqueue(@($response.value[$i].id,"root"))
            }
            
        }
    }

    while($true)
    {
        $inputData = $null
        $inputPeek = $null
        $syncVar.siteQueue.TryDequeue([ref]$inputData)
        if (!$inputData)
        {
            break
        }
        if ($syncVar.activeSites.Contains($inputData)) {
            continue
        }
        $syncVar.activeSites[$poolIndex] = $inputData
        $siteId = $inputData

        $response = Invoke-MgGraphRequest -Uri "https://graph.microsoft.com/beta/sites/$siteId/drives"
        FeedResultToSyncVar -response $response
        while ($response.'@odata.nextLink')
        {
            $response = Invoke-MgGraphRequest -Method GET -Uri $response.'@odata.nextLink'
            FeedResultToSyncVar -response $response
        }
        $syncVar.siteQueue.TryPeek([ref]$inputPeek)
        if (!$inputPeek) {
            break
        }
    }

    $syncVar.activeSites[$poolIndex] = 'NaN'
}

$folderCrawlThread = {
    param($syncVar,$poolIndex)
    $emptyCounter = 0
    $limit = 100
    function FeedResultToSyncVar{
        param($response)
        foreach ($item in $response.value) {
            if ($item.folder)
            {
                $syncVar.folderQueue.Enqueue(@($driveId,$item.id))
            }
            elseif ($item.file)
            {
                $syncVar.files[$item.id] = $item | select id,name,size,parentReference
                $syncVar.fileQueue.Enqueue($item.id)
            }
        }
    }
    while($true)
    {
        $inputData = $null
        $inputPeek = $null
        $syncVar.folderQueue.TryDequeue([ref]$inputData)
        if (!$inputData)
        {
            $emptyCounter++
            if ($emptyCounter -ge $limit) {break}
            Start-Sleep -Milliseconds (Get-Random -Minimum 100 -Maximum 500)
            continue
        }
        if ($syncVar.activeFolders.Contains($inputData)) {
            continue
        }
        $syncVar.activeFolders[$poolIndex] = $inputData
        $driveId = $inputData[0]
        $folderId = $inputData[1]

        $response = Invoke-MgGraphRequest -Method GET -Uri "https://graph.microsoft.com/beta/drives/$driveId/items/$folderId/children"
        FeedResultToSyncVar -response $response
        while($response.'@odata.nextLink')
        {
            $response = Invoke-MgGraphRequest -Method GET -Uri $response.'@odata.nextLink'
            FeedResultToSyncVar -response $response
        }
    }
    $syncVar.activeFolders[$poolIndex] = @('NaN','NaN')
}

$getFileVersionsThread = {
    param($syncVar,$poolIndex)
    function FeedResultToSyncVar
    {
        param($response)
        $values = @($response.value)
        if ($values)
        {
            if (!$syncVar.versions.ContainsKey($fileId))
            {
                $syncVar.versions[$fileId] = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]::New())
            }
            for ($i = 0;$i -lt $values.Count; $i++)
            {
                $syncVar.versions[$fileId].Add($response.value[$i])
            }    
        }
    }
    while($true)
    {
        if ($syncVar.fileQueue.isEmpty)
        {
            if ($syncVar.folderQueue.isEmpty)
            {
                break
            } else
            {
                Start-Sleep -Seconds 5
                continue
            }
        } 
        $inputData = $null
        $inputPeek = $null
        $syncVar.fileQueue.TryDequeue([ref]$inputData)
        if (!$inputData)
        {
            start-sleep -Seconds 5
            continue
        }
        if ($syncVar.activeFiles.Contains($inputData)) {
            continue
        }
        $syncVar.activeFiles[$poolIndex] = $inputData
        $fileId = $inputData
        $driveId = $syncVar.files[$fileId].parentReference.driveId

        $response = Invoke-MgGraphRequest -Method GET -Uri "https://graph.microsoft.com/beta/drives/$driveId/items/$fileId/versions"
        FeedResultToSyncVar -response $response
        while($response.'@odata.nextLink')
        {
            $response = Invoke-MgGraphRequest -Method GET -Uri $response.'@odata.nextLink'
            FeedResultToSyncVar -response $response
        }
    }
    $syncVar.activeFiles[$poolIndex] = 'NaN'
}

####################
# SCRIPTBLOCKS END #
####################

function WaitForAllThreads
{
    start-sleep -seconds 5
    $spin = $true
    $counter = 100
    while($spin) {
        $counter++
        $state = ((get-job).state | sort -Unique)
        $spin = $state -match "NotStarted|Running|Failed|Stopped|Blocked|Suspended|Disconnected|Suspending|Stopping"
        write-host working threads $((get-job | where  {$_.State -match "NotStarted|Running|Failed|Stopped|Blocked|Suspended|Disconnected|Suspending|Stopping"}).count)
        write-host files $syncVar.files.values.count
        write-host versions $syncvar.versions.values.count
        write-host sites $syncVar.sites.values.count
        write-host siteQueue $syncVar.siteQueue.count
        write-host folderQueue $syncVar.folderQueue.count
        write-host folderQueue $syncVar.folderQueue.count
        write-host fileQueue $syncVar.fileQueue.count

        Start-Sleep -seconds 3
        clear-host
    }
    write-host finished
}

$stopwatch =  [system.diagnostics.stopwatch]::StartNew()
for ($i = 0;$i -lt $driveThreads;$i++) {$syncVar.activeDrives.Add('NaN')}
for ($i = 0;$i -lt $driveThreads;$i++) {$syncVar.activeSites.Add('NaN')}
for ($i = 0;$i -lt $folderThreads;$i++) {$syncVar.activeFolders.Add(@('Nan','Nan'))}
for ($i = 0;$i -lt $fileThreads;$i++) {$syncVar.activeFiles.Add('NaN')}

foreach ($item in $targets) {$syncVar.targets.Add($item)}

Start-ThreadJob -ScriptBlock $getSharepointSitesThread -ArgumentList ($syncVar) -Name "Sites Thread" -ThrottleLimit $throttleLimit

WaitForAllThreads

for ($i = 0;$i -lt $driveThreads;$i++)
{
    Start-ThreadJob -ScriptBlock $getSharepointDrivesThread -ArgumentList ($syncVar,$i) -Name "Drives Thread"
}

Start-Sleep -Seconds 3

for ($i = 0;$i -lt $folderThreads;$i++)
{
    Start-ThreadJob -ScriptBlock $folderCrawlThread -ArgumentList ($syncVar,$i) -Name "Folders Thread"
}

start-sleep -seconds 10

for ($i = 0;$i -lt $fileThreads;$i++)
{
    Start-ThreadJob -ScriptBlock $getFileVersionsThread -ArgumentList ($syncVar,$i) -Name "Files Thread"
}

WaitForAllThreads

$stopwatch.Elapsed.TotalSeconds
$stopwatch.Stop()

write-host total size of actual files
(($syncvar.files.values.size) | measure-object -sum).sum / 1GB
write-host total size of the version history
(($syncvar.versions.values | where {$_.count -gt 1}).size | measure-object -sum).sum / 1GB

get-job | remove-job -Force
[System.GC]::Collect()
