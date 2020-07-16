# TgFolderClone
Telegram bot for using folderclone

## Usage
1. Follow instructions at https://github.com/Spazzlo/folderclone on how to create SA accounts.
2. Copy the .json files to **accounts** folder.
3. Rename `config.py.sample` to `config.py`.
4. Edit the values in `config.py`.
5. Run `py folderclone.py`.

## Usage in telegram bot
`/clone <source_id> <dest_id> <thread> <view> <skip>`

`source_id` - Drive ID of the folder you want to copy from. (Required)

`dest_id` - Drive ID of the folder you want to copy into. (Required)

`thread` - Amount of threads to use. The higher the more resource it requires. Default - 10

`view` - 0 - Tree View. 1 - Basic View. Default - Tree View.

`skip` - ID of the folder to skip to. Default - None.

## Details about skipping
This is for those that host bot on heroku. Since heroku recycle their dyno approximately every 24 hr, things might stop halfway during cloning.

Folder IDs are printed along side with the normal progress so you can use it for the skip parameter.

Delete all the files within the folder of which the process stopped at first before cloning again. (So that you won't get duplicates)

Example : `/clone 1234567890abcdefg 1234567890abcdefg 10 0 1234567890abcdefg`

(Yes, you need to specify the rest parameter if you want to use the skip parameter.)


### Note
`folderclone.py` - Modified version of `multifolderclone.py` from https://github.com/justcopy/Multifolderclone

<details>
  <summary>Deploying to Heroku</summary>
  
  ## Concerning thread number
  Recommended thread number for heroku is 25.
  
  40 will cause occasional RAM over usage and stall the app.
  
  50 if you want to risk your app hanging.
  
  Anything above 50 will more likely crash the app.
  
</details>
