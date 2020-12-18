local item_value = os.getenv('item_value')
local item_type = os.getenv('item_type')
local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local discovered = {}

increase_delay = false

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if status_code >= 300 and status_code <= 399 then
    io.stdout:write("Redirects should not happen!\n")
    abortgrab = true
  end

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    io.stdout:flush()
    return wget.actions.ABORT
  end
  
  if status_code == 429 then
    increase_delay = true
  end
  
  if status_code >= 500
    or (status_code >= 400 and status_code ~= 404)
    or status_code  == 0 then
    io.stdout:write("Server returned "..http_stat.statcode.." ("..err.."). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 10
    if tries > maxtries then
      io.stdout:write("\nI give up...\n")
      io.stdout:flush()
      tries = 0
      return wget.actions.ABORT
    else
      if status_code == 429 and tries == 0 then
        delay = 70
      else
        delay = math.floor(math.pow(2, tries))
      end
      os.execute("sleep " .. delay)
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 14

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.write_to_warc = function(url, http_stat)
  return http_stat["statcode"] ~= 429
end

--[[wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  if increase_delay then
    io.stdout:write("Increasing delay by 1 second.\n")
    io.stdout:flush()
    local cur_delay = tonumber(read_file("eurid_delay"))
    -- Check for corruption etc.
    assert(cur_delay > 0 and cur_delay < 30)
    cur_delay = cur_delay + 1
    
    -- Experiementally, it should stop at about 15, but this will constrain it to a sane value if something goes wrong
    if cur_delay > 30 then
      cur_delay = 30
    end
    local file = io.open("eurid_delay", 'w+')
    file:write(tostring(cur_delay))
    file:close()
  end
end]]

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
  
end
