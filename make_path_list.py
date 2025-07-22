import samweb_client
import os,sys


# Define data_dict, a dict of SAM web queries with your choice of keys (will be used in the output file name)
data_dict = { "rolling": "defname: data_MCP2025B_02_DevSample_1e20_bnblight_v10_06_00_02_flatcaf_sbnd and run_number >= 18250 and run_number < 18592 and sbnd.random < 0.02" }
#"initial": "file_type data and data_tier raw and data_stream bnblight and (run_number 18255 or (run_number 18259 and sbnd.random < 0.74))", 
              #"rolling": "defname: data_MCP2025B_02_DevSample_1e20_bnblight_v10_06_00_02_flatcaf_sbnd and ((run_number >= 18250 and run_number != 18255 and run_number != 18259 and run_number < 18592 and sbnd.random < 0.007) or (run_number 18259 and sbnd.random >= 0.74 and sbnd.random < 0.7439))"}
#,               "test": "file_type data and data_tier raw and data_stream bnblight and run_number 18255 and sbnd.random < 0.017" }

# Initialize connection to SAMWeb client
samweb = samweb_client.SAMWebClient(experiment='sbnd')
print("Initialized connection to SAMWeb client")

# Loop over datasets
for key in data_dict:

    print("Doing dataset: ", key)

    # Get list of files in the dataset
    try:
        file_list = samweb.listFiles(data_dict[key])
    #except samweb_client._exceptions.DefinitionNotFound:
    #    print "Definition not found in SAM. Definition: ", dataset
    #    print "Exiting..."
    #    sys.exit(1)
    except:
        print("SAM query failed. Query: ", data_dict[key])
    # Additional error handling
    if not file_list:
        print("Getting file list failed. Dataset ", key, " with query: ", data_dict[key])
        print("Exiting...")
        sys.exit(2)

    # Declare path list to fill
    path_list = []
    url_list = []

    # Loop over files in the dataset
    for f in file_list:

        # Clear location, path variables
        location_out = []
        file_path = ''

        # Locate the file
        try:
            location_out = samweb.locateFile(f)
        except samweb_client._exceptions.FileNotFound:
            print("File not found in SAM. File: ", f)
            print("Exiting...")
            sys.ext(3)
        # Additional error handling
        if not location_out:
            print("Getting file location failed. File: ", f)
            print("Exiting...")
            sys.exit(4)

        # Extract the file's path
        #print(location_out)
        if 'full_path' in location_out[0]:
            path = location_out[0]['full_path'].split(':')[1]
        # Error handling
        else:
            print("File does not have a path in SAM. File: ", f)
            print("Exiting...")
            sys.exit(5)
        # Addtional error handling
        if not path:
            print("Getting file path failed. File: ", f)
            print("Exiting...")
            sys.exit(6)

        # Add file with its path to the list
        path_list.append(os.path.join(path, f))

        # Better yet, extract the file's xrootd URL
        try:
            url = samweb.getFileAccessUrls(f, 'root')[0]
        except:
            print("File does not have a xrootd URL in SAM. File: ", f)
            print("Exiting...")
            sys.exit(7)
        
        # Add URL to the list
        url_list.append(url)


    # Declare the output file to write out to
    output_file = 'file_list_' + key + '.txt'
    # Write the contents of path_list to that file
    with open(output_file, 'w') as f:
        #for path in path_list:
        for path in url_list:
            f.write('{}\n'.format(path))
        
    print("Wrote output file list: ", output_file)

print("Done!")
