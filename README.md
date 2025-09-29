# Meshtastic_File_Transfer
I'm no longer working on this project, if you want this project but with more features, consider checking out [meshtastic_chat_desktop](https://github.com/laneboyerre/meshtastic_chat_desktop) or [RNS_Over_Meshtastic](https://github.com/landandair/RNS_Over_Meshtastic) for which I'm a contributor and creator respectively

System which can be used to send arbitrary binary data files over Meshtastic of a size up to
59kb at a rate between 10-1000 bytes/s which is about as fast as a room full of people on morse code.
The file size can be expanded by sending file chunks of a larger file which can be generated with tools in this repo.

This method of communication can be used by any platform and makes use of a relatively simple but reliable communication protocol as described below.
(Sender[S:] and Receiver[R:])

| Packet Type                                       | Content Description                                              |
|---------------------------------------------------|------------------------------------------------------------------|
| S: Initial Req.                                   | text: !fcom,file:r"file_name",packets:(# Packets),id:(id)        |
| R: Accept Transfer                                | Data: bytes(f, c, o, m, (file_id), 1)                            |
| S: Send data in chunks                            | Data: bytes((file_id), (Packet_Index), 232 bytes max payload...) |
| S: Finished Transmitting                          | Data: bytes(f, c, o, m, (file_id), 2)                            |
| R: (If packets missing) Req. Retransmission       | Data: bytes(f, c, o, m, (file_id), 3, missing packet ids...)     |
| R: (If packets are all present) Transfer Finished | Data: bytes(f, c, o, m, (file_id), 4)                            |
| Finished Transmitting                             | NONE                                                             |

Note that Req. Retransmission packets should just cause the sender to send the missing packets then another Finished transmitting packet.
This cycle will repeat until a com packet is missed or all the data packets are received.

Also, the file ID number must never be byte(f) or 102 in base 10.
# Command line tools
The `File_Tools` directory now exposes three small utilities to help prepare data for transport.
Each utility validates input and provides clear error messages so that failures are easier to diagnose.

`File_Compression.py`
Compress audio, image, or arbitrary files.
Audio is transcoded to a mono MP3.
Images are downscaled and exported as WebP.
Other files are wrapped in a zip archive.

`File_Splitter.py`
Split a large file into chunked segments suitable for staggered transfers.
The default chunk size mirrors the transfer packet layout, and can be overridden with a command-line flag.

`File_Combiner.py`
Recombine numbered file parts back into a single artifact once all segments arrive.
The tool infers the output name automatically from the directory.
#Instructions:Main_2way.py
- Plug in 2 radios into one or two computers
- Start the Receiver script with the desired cmd line args
- Start the Sender script with the file or directory as a cmdline arg
- Set the destination to the other radio when prompted
- Progress Bar should show up and begin filling if everything is working properly
