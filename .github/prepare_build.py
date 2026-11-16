"""Run by the release workflow before building: stamps the release version into the app and writes the Windows
version-info resource (company/product/version shown in the exe's Properties), which makes the exe look like what it is."""
import os
import re
import sys

tag = os.environ.get("RELEASE_TAG", "server-v0.0.0")
version = tag.split("server-v", 1)[-1].lstrip("vV") or "0.0.0"
nums = [int(re.sub(r"\D", "", p) or 0) for p in version.split(".")][:4]
nums += [0] * (4 - len(nums))

p = "fuads_server_core.py"
s = open(p, encoding="utf-8", newline="").read()
s, n = re.subn(r'^APP_VERSION = "[^"]*"', f'APP_VERSION = "{version}"', s, count=1, flags=re.M)
if n != 1:
    sys.exit("APP_VERSION line not found")
open(p, "w", encoding="utf-8", newline="").write(s)

tup = ", ".join(map(str, nums))
info = f'''VSVersionInfo(
  ffi=FixedFileInfo(filevers=({tup}), prodvers=({tup}), mask=0x3f, flags=0x0, OS=0x40004, fileType=0x1, subtype=0x0, date=(0, 0)),
  kids=[
    StringFileInfo([StringTable("040904B0", [
      StringStruct("CompanyName", "Fuad"),
      StringStruct("FileDescription", "Fuad's Server - local project server for Mentor Workstation"),
      StringStruct("FileVersion", "{version}"),
      StringStruct("InternalName", "Fuads_Server"),
      StringStruct("LegalCopyright", "Fuad. Open source: github.com/itsfuad41/Mentor-Workspaces-GHIT"),
      StringStruct("OriginalFilename", "Fuads_Server.exe"),
      StringStruct("ProductName", "Fuad's Server"),
      StringStruct("ProductVersion", "{version}")])]),
    VarFileInfo([VarStruct("Translation", [1033, 1200])])
  ])
'''
open("version_info.txt", "w", encoding="utf-8").write(info)
print("Prepared build for version", version)
