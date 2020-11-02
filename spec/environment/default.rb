# encoding: UTF-8
#
# Copyright (c) 2010-2015 GoodData Corporation. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

module GoodData
  module Connectors
    module DownloaderCsv
      module ConnectionHelper
        set_const :DEFAULT_BDS_BUCKET, 'msf-dev-grest'
        set_const :DEFAULT_BDS_FOLDER, 'ETL_tests/connectors_downloader_csv'
        set_const :DEFAULT_TIMESTAMP, Time.now.strftime('unit_tests_%Y_%m_%d_%H_%M_%s').freeze
        set_const :DEFAULT_BDS_ACCESS_KEY, ENV['S3_ACCESS_KEY'] || GoodData::Helpers.decrypt(GoodData::Helpers::AuthHelper.read_credentials(credentials_file_path = 'spec/data/.gooddata')[:s3_access_key], ENV['GD_SPEC_PASSWORD'] || ENV['BIA_ENCRYPTION_KEY'])
        set_const :DEFAULT_BDS_SECRET_KEY, ENV['S3_SECRET_KEY'] || GoodData::Helpers.decrypt(GoodData::Helpers::AuthHelper.read_credentials(credentials_file_path = 'spec/data/.gooddata')[:s3_secret_key], ENV['GD_SPEC_PASSWORD'] || ENV['BIA_ENCRYPTION_KEY'])
        set_const :CFG_BDS_ACCESS_KEY, GoodData::Helpers.decrypt(GoodData::Helpers::AuthHelper.read_credentials(credentials_file_path = 'spec/data/.gooddata')[:cfg_s3_access_key], ENV['GD_SPEC_PASSWORD'] || ENV['BIA_ENCRYPTION_KEY'])
        set_const :CFG_BDS_SECRET_KEY, GoodData::Helpers.decrypt(GoodData::Helpers::AuthHelper.read_credentials(credentials_file_path = 'spec/data/.gooddata')[:cfg_s3_secret_key], ENV['GD_SPEC_PASSWORD'] || ENV['BIA_ENCRYPTION_KEY'])
        set_const :OD_TOKEN, GoodData::Helpers.decrypt(GoodData::Helpers::AuthHelper.read_credentials(credentials_file_path = 'spec/data/.gooddata')[:onedrive_token], ENV['GD_SPEC_PASSWORD'] || ENV['BIA_ENCRYPTION_KEY'])
        set_const :STORAGE_PASSWORD, GoodData::Helpers.decrypt(GoodData::Helpers::AuthHelper.read_credentials(credentials_file_path = 'spec/data/.gooddata')[:storage_password], ENV['GD_SPEC_PASSWORD'] || ENV['BIA_ENCRYPTION_KEY'])
        set_const :PGP_PASSPHRASE, 'Ng5AKenqrUCCsOs4RAEJF5Fw+gTroCicqeJK3M6K'

        set_const :DEFAULT_DOWNLOADER, 'csv_downloader_1'

        set_const :PARAMS, 'bds_bucket' => DEFAULT_BDS_BUCKET,
                           'bds_folder' => DEFAULT_BDS_FOLDER + '/' + DEFAULT_TIMESTAMP,
                           'bds_access_key' => DEFAULT_BDS_ACCESS_KEY,
                           'bds_secret_key' => DEFAULT_BDS_SECRET_KEY,
                           'csv|options|access_key' => DEFAULT_BDS_ACCESS_KEY,
                           'csv|options|secret_key' => DEFAULT_BDS_SECRET_KEY,
                           'csv|options|token' => OD_TOKEN,
                           'ID' => DEFAULT_DOWNLOADER,
                           # 'GDC_LOGGER' =>Logger.new(STDOUT)
                           'GDC_LOGGER' => Logger.new(File.open(File::NULL, 'w'))

        set_const :STAGING_2_ENVIRONMENT, 'WEBDAV_HOST'     => 'https://na1-staging2-di.intgdc.com',
                                          'WEBDAV_URL'      => 'https://na1-staging2-di.intgdc.com/uploads/',
                                          'WEBDAV_USERNAME' => 'qa+test@gooddata.com',
                                          'WEBDAV_PASSWORD' => STORAGE_PASSWORD,
                                          'SFTP_HOST'       => 'na1-staging2-di.intgdc.com',
                                          'SFTP_USERNAME'   => 'qa+test@gooddata.com',
                                          'SFTP_PASSWORD'   => STORAGE_PASSWORD

        set_const :STAGING_3_ENVIRONMENT, 'WEBDAV_HOST'     => 'https://na1-staging3-di.intgdc.com',
                                          'WEBDAV_URL'      => 'https://na1-staging3-di.intgdc.com/uploads/',
                                          'WEBDAV_USERNAME' => 'qa+test@gooddata.com',
                                          'WEBDAV_PASSWORD' => STORAGE_PASSWORD,
                                          'SFTP_HOST'       => 'na1-staging3-di.intgdc.com',
                                          'SFTP_USERNAME'   => 'qa+test@gooddata.com',
                                          'SFTP_PASSWORD'   => STORAGE_PASSWORD

        set_const :ENVIRONMENTS,  'staging_2' => STAGING_2_ENVIRONMENT,
                                  'staging_3' => STAGING_3_ENVIRONMENT

        set_const :TESTING_ENVIRONMENT, ENVIRONMENTS[ENV['GD_ENV']] || STAGING_2_ENVIRONMENT

        set_const :WEBDAV_HOST, TESTING_ENVIRONMENT['WEBDAV_HOST']
        set_const :WEBDAV_URL, TESTING_ENVIRONMENT['WEBDAV_URL']
        set_const :WEBDAV_USERNAME, TESTING_ENVIRONMENT['WEBDAV_USERNAME']
        set_const :WEBDAV_PASSWORD, TESTING_ENVIRONMENT['WEBDAV_PASSWORD']

        set_const :SFTP_HOST, TESTING_ENVIRONMENT['SFTP_HOST']
        set_const :SFTP_USERNAME, TESTING_ENVIRONMENT['SFTP_USERNAME']
        set_const :SFTP_PASSWORD, TESTING_ENVIRONMENT['SFTP_PASSWORD']

      end
    end
  end
end
