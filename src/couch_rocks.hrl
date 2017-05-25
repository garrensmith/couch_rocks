% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

% -record(st, {
%     dirpath,
%     cp_fd,
%     idx_fd,
%     data_fd,
%     fd_monitors,
%     fsync_options,
%     header,
%     needs_commit,
%     id_tree,
%     seq_tree,
%     local_tree,
%     compression
% }).

-record(state, {
    db_handle,
    id_handle,
    seq_handle,
    meta_handle,
    file_name
    % cp_fd,
    % idx_fd,
    % data_fd,
    % fd_monitors,
    % fsync_options,
    % header,
    % needs_commit,
    % id_tree,
    % seq_tree,
    % local_tree,
    % compression
}).

