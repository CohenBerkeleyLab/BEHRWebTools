function [  ] = behr_hdf2txt( hdffile, txtpath, varargin )
% BEHR_HDF2TXT - Convert a BEHR HDF file to a CSV file
%   BEHR_HDF2TXT( HDFFILE, TXTPATH ) Reads all variables in the .hdf file
%   HDFFILE and saves them to separate .csv files for each swath in the
%   directory given by TXTPATH.
%
%   Each pixel is a row in the .csv file. The output files always contain the 
%   swath number, along track index, and across track index as the first three
%   entries in each row. The indices are 0-based; the intent is to make it easy 
%   to reconstruct the spatial dimensions of the swath. For 3D variables that 
%   have multiple values per pixel, each vertical level gets its own column;
%   e.g. Loncorn has four values per pixel, so it splits into 4 fields: 
%   Loncorn01, Loncorn02, Loncorn03, and Loncorn04.
%
%   BEHR_HDF2TXT( HDFFILE, TXTPATH, VARS ) Reads only the varables listed
%   in VARS into the .csv files. VARS may either be a string (for a single
%   variable) or a cell array of strings (for multiple variables).
%
%   BEHR_HDF2TXT( ___, 'nosplit' ) Saves all swaths in HDFFILE to a single
%   .csv file. This can get fairly large.

%%%%% INPUT VALIDATION %%%%%

split_swaths = true;
vars = {};
for a=1:numel(varargin)
    if strcmpi(varargin{a}, 'nosplit')
        split_swaths = false;
    else
        vars = varargin{a};
    end
end

if ischar(hdffile)
    if exist(hdffile, 'file')
        try
            hi = h5info(hdffile);
        catch err
            if strcmpi(err.identifier, 'MATLAB:imagesci:h5info:libraryError')
                error('bad_input:hdf_file','%s does not appear to be a valid HDF5 file', hdffile)
            else
                rethrow(err)
            end
        end
    else
        error('bad_input:hdf_file','%s does not exist', hdffile);
    end
else
    error('bad_input:wrong_type','HDFFILE should be a string');
end

if ~exist(txtpath,'dir')
    error('bad_input:txt_path','TXTPATH (%s) does not exist', txtpath)
end

if isempty(vars)
    vars = list_datasets(hi);
elseif ischar(vars)
    vars = {vars};
elseif ~iscellstr(vars)
    error('bad_input:wrong_type','VARS must be a string or cell array of strings, if given');
end

split_swaths = false;

%%%%% MAIN FUNCTION %%%%%

[~, file_name] = fileparts(hi.Filename);

nswaths = numel(hi.Groups.Groups);

% Generate header such that any 3D variables have and entry for each level
% Assume this is constant across all swaths
header = cell(1,100); % we'll cut it down later
header(1:3) = {'Swath','AlongTrackInd','AcrossTrackInd'};
var3d = false(size(vars));
i = 4;
for a=1:numel(vars)
    ind = dset_info(hi.Groups.Groups(1), vars{a});
    sz = hi.Groups.Groups(1).Datasets(ind).Dataspace.Size;
    if length(sz) > 2
        for b=1:sz(1)
            header{i} = sprintf('%s-%02d',vars{a},b);
            i=i+1;
        end
        var3d(a) = true;
    else
        header{i} = vars{a};
        i = i+1;
    end
    
end
header(i:end) = [];
headerstr = strcat(strjoin(header,','),'\n');

sz_array = zeros(nswaths,2);
for a=1:nswaths
    ind = dset_info(hi.Groups.Groups(a), 'Longitude');
    sz_array(a,:) = hi.Groups.Groups(a).Datasets(ind).Dataspace.Size;
end

for a=1:nswaths
    sz = sz_array(a,:);
    [~, swath] = fileparts(hi.Groups.Groups(a).Name);
    swathnum = regexp(swath,'\d+','match','once');
    if a == 1 || split_swaths
        if split_swaths
            txtname = sprintf('%s_%s.csv',file_name,swath);
            topinfo = sprintf('AlongTrackDim=%d\nAcrossTrackDim=%d\n',sz(1),sz(2));
        else
            txtname = sprintf('%s.csv',file_name);
            ltrack = num2cell(sz_array(:,1));
            xtrack = num2cell(sz_array(:,2));
            fmtspec = strjoin(repmat({'%d'},1,nswaths),',');
            topinfospec = sprintf('AlongTrackDim=%1$s\\nAcrossTrackDim=%1$s\\n',fmtspec);
            topinfo = sprintf(topinfospec, ltrack{:}, xtrack{:});
        end
        txtfile = fullfile(txtpath, txtname);
        fid = fopen(txtfile,'w');
        
        
        fprintf(fid, topinfo);
        fprintf(fid,headerstr);
    end
    
    data = cell(size(vars));
    
    for b=1:prod(sz)
        [x,y] = ind2sub(sz,b);
        x = x-1; y = y-1; % change to 0 based indexing. This is consistent with the Python utility.
        fline = cell(size(header));
        fline(1:3) = {swathnum, num2str(x), num2str(y)};
        
        i=4;
        for c=1:numel(vars)
            [~, dset_name] = dset_info(hi.Groups.Groups(a), vars{c});
            if b==1
                % Load the variables
                data{c} = h5read(hi.Filename, dset_name);
            end
            if var3d(c)
                val = val2cell(data{c}(:,b)');
                j = i + length(val) - 1;
                fline(i:j) = val;
                i = j+1;
            else
                fline(i) = val2cell(data{c}(b));
                i = i+1;
            end
        end
        
        fline = strcat(strjoin(fline,','),'\n');
        fprintf(fid, fline);
    end
    
    if a == nswaths || split_swaths
        fclose(fid);
    end
end


end

function dsets = list_datasets(hinfo)
% Returns a cell array listing the datasets in the BEHR .hdf file. Assumes
% that the datasets are the same in each swath
if strcmp(hinfo.Name, '/')
    dsets = {hinfo.Groups.Groups(1).Datasets.Name}';
else
    dsets = {hinfo.Datasets.Name}';
end
end

function [ind, name] = dset_info(group, dset_name)
dsets = list_datasets(group);
for ind=1:numel(dsets)
    if strcmp(group.Datasets(ind).Name,dset_name)
        name = strcat(group.Name,'/',dset_name);
        return
    end
end
error('h5group:dataset_not_found','Could not find the dataset %s in %s', dset_name, group.name);
end

function c = val2cell(val)
c = num2cell(val);
if ~isrow(c);
    c = c';
end
for a=1:numel(c)
    c{a} = sprintf('%.4g',c{a});
end
end

% function response = ask_yn(prompt)
% full_prompt = sprintf('%s (y/n)',prompt);
% while true
%     user_ans = input(full_prompt,'s');
%     if strcmpi(user_ans, 'y') || strcmpi(user_ans, 'n')
%         response = strcmpi(user_ans, 'y');
%         return
%     else
%         fprintf('Enter y or n only. ');
%     end
% end
% end
