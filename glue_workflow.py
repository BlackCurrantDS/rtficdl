import aws_cdk.aws_glue as glue_
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3_deployment as s3_deployment
import aws_cdk.aws_sns as sns
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as etargets
from aws_cdk import core


class InitGlueObjects:
    """ Define all crawlers for script your stack.

        Usage: create an instance like "dp_crawler = InitCrawlers(self, ...)" to reference the here definied objects.

    """

    def __init__(stack, self,
                # Buckets
                s3_rtf
                dp_lambda,  # Lambdas
                dp_crawler,  # Crawler
                stack_params
                 ):
        """
        Input parameters:       All objectes from your stack, which are referrenced here.
        :param self:            This is the 'self' from the invoking class instance of 'core.Stack'
        :param s3_rtf:  all needed objcets from invoking instance.
        :param et cetera...
        """

        print('## GLUE: definition start  ##')

        # Config ####################

        python_version = '3'
        glue_version1 = '1.0'  # python shell
        jobtype_pythonshell = 'pythonshell'
        glue_version2 = '2.0'  # pyspark, packages see: https://docs.aws.amazon.com/glue/latest/dg/reduced-start-times-spark-etl-jobs.html
        jobtype_glueetl = 'glueetl'  # glueetl - spark - simple python

        # job role
        glue_job_role = iam.Role(self, 'glue_job_role',
                                 assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
                                 description='Role for Glue Jobs',
                                 role_name='',
                              
                                 )
       
        s3_rtf.grant_read_write(glue_job_role)


        #for gdpr
        allow_glue_get_gdpr_secrets_policy = iam.Policy(self, 'glue_gdpr_secret_manager_policy',
                                                   policy_name='',
                                                   statements=[iam.PolicyStatement(
                                                       actions=["secretsmanager:GetSecretValue",
                                                                "secretsmanager:DescribeSecret"],
                                                       effect=iam.Effect.ALLOW,
                                                       resources=[
                                                           f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:?-??????"]
                                                   )
                                                   ]
                                                   )
        glue_job_role.attach_inline_policy(allow_glue_get_gdpr_secrets_policy)

        
        # RTF Jobs - These jobs are for right to be forgotten GDPR
        extra_py_files_rtf = ','.join([
            s3_glue_fn.s3_url_for_object('rtf/rtf_helpers/rtf_get_glue_metadata.py'),
            s3_glue_fn.s3_url_for_object('rtf/rtf_helpers/rtf_job_helpers.py'),
            s3_glue_fn.s3_url_for_object('rtf/rtf_helpers/rtf_datalake_update_helpers.py'),
            s3_glue_fn.s3_url_for_object('rtf/rtf_helpers/rtf_redshift_mapping_metadata.py'),
            s3_glue_fn.s3_url_for_object('rtf/config_files/rtf_initial_config_file.py')
        ])

        #get metadata for redshift and data lake

        rtf_create_initial_config_file_id = 'dp_rtf_create_initial_config_file'
        rtf_create_initial_config_file_cmd = glue_.CfnJob.JobCommandProperty(name=jobtype_glueetl,
                                                                      python_version=python_version,
                                                                      script_location=s3_glue_fn.s3_url_for_object(
                                                                          "rtf/rtf_create_initial_config_file.py")
                                                                      )
        rtf_create_initial_config_file = glue_.CfnJob(self, rtf_create_initial_config_file_id,
                                               name=rtf_create_initial_config_file_id,
                                               role=glue_job_role.role_arn,
                                               command=rtf_create_initial_config_file_cmd,
                                               glue_version=glue_version2,
                                               timeout=60,  # DE: 30 minutes
                                               default_arguments={
                                                   '--workflow_name': 'dp_workflow_rtf_get_user_data',
                                                   '--s3_rtf': str(s3_rtf.bucket_name),
                                                   '--extra-py-files': extra_py_files_rtf,
                                                   '--additional-python-modules': 'smart-open',
                                               }
                                               )
        rtf_get_metadata_config_id = 'dp_rtf_metadata_config'
        rtf_get_metadata_config_cmd = glue_.CfnJob.JobCommandProperty(name=jobtype_glueetl,
                                                                                    python_version=python_version,
                                                                                    script_location=s3_glue_fn.s3_url_for_object(
                                                                                        "rtf/rtf_get_metadata_config.py")
                                                                                    )
        rtf_get_metadata_config_conn = glue_.CfnJob.ConnectionsListProperty(
            connections=['gdpr_rtf_dwh_conn']
        )

        rtf_get_metadata_config = glue_.CfnJob(self, rtf_get_metadata_config_id,
                                                             name=rtf_get_metadata_config_id,
                                                             role=glue_job_role.role_arn,
                                                             command=rtf_get_metadata_config_cmd,
                                                             glue_version=glue_version2,
                                                             timeout=60,  # DE: 30 minutes
                                                             connections=rtf_get_metadata_config_conn,
                                                             default_arguments={
                                                                 '--TempDir': f's3://{str(s3_rtf.bucket_name)}/redshift_temp/',
                                                                 '--workflow_name': 'dp_workflow_rtf_get_user_data',
                                                                 '--s3_rtf': str(s3_rtf.bucket_name),
                                                                 '--extra-py-files': extra_py_files_rtf,
                                                                 '--additional-python-modules': 'smart-open,pg8000',
                                                             }
                                                             )

        #get user data from datalake
        rtf_get_user_data_from_datalake_id = 'dp_rtf_get_user_data_from_datalake'
        rtf_get_user_data_from_datalake_cmd = glue_.CfnJob.JobCommandProperty(name=jobtype_glueetl,
                                                                      python_version=python_version,
                                                                      script_location=s3_glue_fn.s3_url_for_object(
                                                                          "rtf/rtf_get_user_data_from_datalake.py")
                                                                      )
        rtf_get_user_data_from_datalake_conn = glue_.CfnJob.ConnectionsListProperty(
            connections=['gdpr_rtf_dwh_conn']
        )

        rtf_get_user_data_from_datalake = glue_.CfnJob(self, rtf_get_user_data_from_datalake_id,
                                               name=rtf_get_user_data_from_datalake_id,
                                               role=glue_job_role.role_arn,
                                               command=rtf_get_user_data_from_datalake_cmd,
                                               glue_version=glue_version2,
                                               timeout=60,  # DE: 30 minutes
                                               connections=rtf_get_user_data_from_datalake_conn,
                                               default_arguments={
                                                   '--TempDir': f's3://{str(s3_rtf.bucket_name)}/redshift_temp/',
                                                   '--workflow_name': 'dp_workflow_rtf_get_user_data',
                                                   '--s3_rtf': str(s3_rtf.bucket_name),
                                                   '--extra-py-files': extra_py_files_rtf,
                                                   '--additional-python-modules': 'smart-open,pg8000',
                                               }
                                               )
        # get user data from redshift
        rtf_get_user_data_from_redshift_id = 'dp_rtf_get_user_data_from_redshift'
        rtf_get_user_data_from_redshift_cmd = glue_.CfnJob.JobCommandProperty(name=jobtype_glueetl,
                                                                              python_version=python_version,
                                                                              script_location=s3_glue_fn.s3_url_for_object(
                                                                                  "rtf/rtf_get_user_data_from_redshift.py")
                                                                              )
        rtf_get_user_data_from_redshift_conn = glue_.CfnJob.ConnectionsListProperty(
            connections=['gdpr_rtf_dwh_conn']
        )

        rtf_get_user_data_from_redshift = glue_.CfnJob(self, rtf_get_user_data_from_redshift_id,
                                                       name=rtf_get_user_data_from_redshift_id,
                                                       role=glue_job_role.role_arn,
                                                       command=rtf_get_user_data_from_redshift_cmd,
                                                       glue_version=glue_version2,
                                                       timeout=60,  # DE: 30 minutes
                                                       connections=rtf_get_user_data_from_redshift_conn,
                                                       default_arguments={
                                                           '--TempDir': f's3://{str(s3_rtf.bucket_name)}/redshift_temp/',
                                                           '--workflow_name': 'dp_workflow_rtf_get_user_data',
                                                           '--s3_rtf': str(s3_rtf.bucket_name),
                                                           '--extra-py-files': extra_py_files_rtf,
                                                           '--additional-python-modules': 'smart-open,pg8000',
                                                       }
                                                       )

        #get user data report
        rtf_get_user_html_report_id = 'dp_rtf_get_user_html_report'
        rtf_get_user_html_report_cmd = glue_.CfnJob.JobCommandProperty(name=jobtype_glueetl,
                                                                              python_version=python_version,
                                                                              script_location=s3_glue_fn.s3_url_for_object(
                                                                                  "rtf/rtf_get_user_html_report.py")
                                                                              )
        rtf_get_user_html_report_conn = glue_.CfnJob.ConnectionsListProperty(
            connections=['gdpr_rtf_dwh_conn']
        )

        rtf_get_user_html_report = glue_.CfnJob(self, rtf_get_user_html_report_id,
                                                       name=rtf_get_user_html_report_id,
                                                       role=glue_job_role.role_arn,
                                                       command=rtf_get_user_html_report_cmd,
                                                       glue_version=glue_version2,
                                                       timeout=60,  # DE: 30 minutes
                                                       connections=rtf_get_user_html_report_conn,
                                                       default_arguments={
                                                           '--TempDir': f's3://{str(s3_rtf.bucket_name)}/redshift_temp/',
                                                           '--workflow_name': 'dp_workflow_rtf_get_user_data',
                                                           '--s3_rtf': str(s3_rtf.bucket_name),
                                                           '--extra-py-files': extra_py_files_rtf,
                                                           '--additional-python-modules': 'jinja2,smart-open,pg8000',
                                                       }
                                                       )
        # update datalake for user
        rtf_update_datalake_for_user_id = 'dp_rtf_update_datalake_for_user'
        rtf_update_datalake_for_user_cmd = glue_.CfnJob.JobCommandProperty(name=jobtype_glueetl,
                                                                              python_version=python_version,
                                                                              script_location=s3_glue_fn.s3_url_for_object(
                                                                                  "rtf/rtf_update_datalake.py")
                                                                              )

        rtf_update_datalake_for_user = glue_.CfnJob(self, rtf_update_datalake_for_user_id,
                                                       name=rtf_update_datalake_for_user_id,
                                                       role=glue_job_role.role_arn,
                                                       command=rtf_update_datalake_for_user_cmd,
                                                       glue_version=glue_version2,
                                                       timeout=60,  # DE: 30 minutes
                                                       default_arguments={
                                                           '--workflow_name': 'dp_workflow_rtf_mask_user_data',
                                                           '--s3_rtf': str(s3_rtf.bucket_name),
                                                           '--extra-py-files': extra_py_files_rtf,
                                                           '--additional-python-modules': 'bs4,smart-open,pg8000,html5lib',
                                                       }
                                                       )

        # get redshift sql update file
        rtf_get_redshift_sql_update_id = 'dp_rtf_get_redshift_sql_update'
        rtf_get_redshift_sql_update_cmd = glue_.CfnJob.JobCommandProperty(name=jobtype_glueetl,
                                                                           python_version=python_version,
                                                                           script_location=s3_glue_fn.s3_url_for_object(
                                                                               "rtf/rtf_get_redshift_sql_update.py")
                                                                           )

        rtf_get_redshift_sql_update = glue_.CfnJob(self, rtf_get_redshift_sql_update_id,
                                                    name=rtf_get_redshift_sql_update_id,
                                                    role=glue_job_role.role_arn,
                                                    command=rtf_get_redshift_sql_update_cmd,
                                                    glue_version=glue_version2,
                                                    timeout=60,  # DE: 30 minutes
                                                    default_arguments={
                                                        '--workflow_name': 'dp_workflow_rtf_mask_user_data',
                                                        '--s3_rtf': str(s3_rtf.bucket_name),
                                                        '--extra-py-files': extra_py_files_rtf,
                                                        '--additional-python-modules': 'bs4,smart-open,pg8000,html5lib',
                                                    }
                                                    )

        rtf_run_sql_update_script_conn = glue_.CfnJob.ConnectionsListProperty(
            connections=['gdpr_rtf_dwh_conn']
        )

        # run update sql script
        rtf_run_sql_update_script_id = 'dp_rtf_run_sql_update_script'
        rtf_run_sql_update_script_cmd = glue_.CfnJob.JobCommandProperty(name=jobtype_glueetl,
                                                                          python_version=python_version,
                                                                          script_location=s3_glue_fn.s3_url_for_object(
                                                                              "rtf/rtf_run_sql_update_script.py")
                                                                          )

        rtf_run_sql_update_script = glue_.CfnJob(self, rtf_run_sql_update_script_id,
                                                   name=rtf_run_sql_update_script_id,
                                                   role=glue_job_role.role_arn,
                                                   command=rtf_run_sql_update_script_cmd,
                                                    connections=rtf_run_sql_update_script_conn,
                                                   glue_version=glue_version2,
                                                   timeout=60,  # DE: 30 minutes
                                                   default_arguments={
                                                       '--TempDir': f's3://{str(s3_rtf.bucket_name)}/redshift_temp/',
                                                       '--workflow_name': 'dp_workflow_rtf_mask_user_data',
                                                       '--s3_rtf': str(s3_rtf.bucket_name),
                                                       '--extra-py-files': extra_py_files_rtf,
                                                       '--additional-python-modules': 'bs4,smart-open,pg8000,html5lib',
                                                   }
                                                   )
        # delete backup file for user
        rtf_delete_backup_file_for_user_id = 'dp_rtf_delete_backup_file_for_user'
        rtf_delete_backup_file_for_user_cmd = glue_.CfnJob.JobCommandProperty(name=jobtype_glueetl,
                                                                           python_version=python_version,
                                                                           script_location=s3_glue_fn.s3_url_for_object(
                                                                               "rtf/rtf_delete_backup_files.py")
                                                                           )

        rtf_delete_backup_file_for_user = glue_.CfnJob(self, rtf_delete_backup_file_for_user_id,
                                                    name=rtf_delete_backup_file_for_user_id,
                                                    role=glue_job_role.role_arn,
                                                    command=rtf_delete_backup_file_for_user_cmd,
                                                    glue_version=glue_version2,
                                                    timeout=60,  # DE: 30 minutes
                                                    default_arguments={
                                                        '--workflow_name': 'dp_workflow_rtf_mask_user_data',
                                                        '--s3_rtf': str(s3_rtf.bucket_name),
                                                        '--extra-py-files': extra_py_files_rtf,
                                                        '--additional-python-modules': 'bs4,smart-open,pg8000,html5lib',
                                                    }
                                                    )

        # RTF search Workflow
        workflow_rtf_get_user_data_id = 'dp_workflow_rtf_get_user_data'

        workflow_rtf_get_user_data = glue_.CfnWorkflow(self, workflow_rtf_get_user_data_id,
                                                           name=workflow_rtf_get_user_data_id,
                                                           description='Getting user data from DataLake and Redshift.',
                                                           tags={'dataobject': 'Right to be forgotten'}
                                                           )

        trigger_rtf_create_initial_config_file_id = 'dp_trigger_rtf_create_initial_config_file'
        trigger_rtf_create_initial_config_file = glue_.CfnTrigger(self, trigger_rtf_create_initial_config_file_id,
                                                         name=trigger_rtf_create_initial_config_file_id,
                                                         description='Creates initial config file',
                                                         workflow_name=workflow_rtf_get_user_data.name,
                                                         type='ON_DEMAND',
                                                         actions=[glue_.CfnTrigger.ActionProperty(
                                                             job_name=rtf_create_initial_config_file.name)
                                                         ],
                                                         start_on_creation=False,
                                                         )

        trigger_rtf_get_metadata_config_id = 'dp_trigger_rtf_get_metadata_config'
        trigger_rtf_get_metadata_config = glue_.CfnTrigger(self, trigger_rtf_get_metadata_config_id,
                                                                name=trigger_rtf_get_metadata_config_id,
                                                                description='Generates Meta data',
                                                                workflow_name=workflow_rtf_get_user_data.name,
                                                                type='CONDITIONAL',
                                                                predicate={'conditions': [
                                                                    glue_.CfnTrigger.ConditionProperty(
                                                                        job_name=rtf_create_initial_config_file.name,
                                                                        state='SUCCEEDED',
                                                                        logical_operator='EQUALS')
                                                                ]},
                                                                actions=[glue_.CfnTrigger.ActionProperty(
                                                                    job_name=rtf_get_metadata_config.name)
                                                                ],
                                                                start_on_creation=True,
                                                                )

        trigger_rtf_get_user_data_from_datalake_id = 'dp_trigger_rtf_get_user_data_from_datalake'
        trigger_rtf_get_user_data_from_datalake = glue_.CfnTrigger(self, trigger_rtf_get_user_data_from_datalake_id,
                                                           name=trigger_rtf_get_user_data_from_datalake_id,
                                                           description='Get User data from data lake',
                                                           workflow_name=workflow_rtf_get_user_data.name,
                                                           type='CONDITIONAL',
                                                           predicate={'conditions': [
                                                               glue_.CfnTrigger.ConditionProperty(
                                                                   job_name=rtf_get_metadata_config.name,
                                                                   state='SUCCEEDED',
                                                                   logical_operator='EQUALS')
                                                           ]},
                                                           actions=[glue_.CfnTrigger.ActionProperty(
                                                               job_name=rtf_get_user_data_from_datalake.name)
                                                           ],
                                                           start_on_creation=True,
                                                           )
        trigger_rtf_get_user_data_from_redshift_id = 'dp_trigger_rtf_get_user_data_from_redshift'
        trigger_rtf_get_user_data_from_redshift = glue_.CfnTrigger(self, trigger_rtf_get_user_data_from_redshift_id,
                                                                   name=trigger_rtf_get_user_data_from_redshift_id,
                                                                   description='Searches and extract user data from redshift',
                                                                   workflow_name=workflow_rtf_get_user_data.name,
                                                                   type='CONDITIONAL',
                                                                   predicate={'conditions': [
                                                                       glue_.CfnTrigger.ConditionProperty(
                                                                           job_name=rtf_get_metadata_config.name,
                                                                           state='SUCCEEDED',
                                                                           logical_operator='EQUALS')
                                                                   ]},
                                                                   actions=[glue_.CfnTrigger.ActionProperty(
                                                                       job_name=rtf_get_user_data_from_redshift.name)
                                                                   ],
                                                                   start_on_creation=True,
                                                                   )

        trigger_rtf_get_user_html_report_id = 'dp_trigger_rtf_get_user_html_report'
        trigger_rtf_get_user_html_report = glue_.CfnTrigger(self, trigger_rtf_get_user_html_report_id,
                                                                   name=trigger_rtf_get_user_html_report_id,
                                                                   description='Generates Complete user data report in html',
                                                                   workflow_name=workflow_rtf_get_user_data.name,
                                                                   type='CONDITIONAL',
                                                                   predicate={'conditions': [
                                                                       glue_.CfnTrigger.ConditionProperty(
                                                                           job_name=rtf_get_user_data_from_redshift.name,
                                                                           state='SUCCEEDED',
                                                                           logical_operator='EQUALS') and
                                                                       glue_.CfnTrigger.ConditionProperty(
                                                                           job_name=rtf_get_user_data_from_datalake.name,
                                                                           state='SUCCEEDED',
                                                                           logical_operator='EQUALS')
                                                                   ]},
                                                                   actions=[glue_.CfnTrigger.ActionProperty(
                                                                       job_name=rtf_get_user_html_report.name)
                                                                   ],
                                                                   start_on_creation=True,
                                                                   )

        workflow_rtf_mask_user_data_id = 'dp_workflow_rtf_mask_user_data'

        workflow_rtf_mask_user_data = glue_.CfnWorkflow(self, workflow_rtf_mask_user_data_id,
                                                       name=workflow_rtf_mask_user_data_id,
                                                       description='Masking user data from DataLake and Redshift.',
                                                       tags={'dataobject': 'Right to be forgotten'}
                                                       )
        trigger_rtf_update_datalake_for_user_id = 'dp_trigger_rtf_update_datalake_for_user'
        trigger_rtf_update_datalake_for_user = glue_.CfnTrigger(self, trigger_rtf_update_datalake_for_user_id,
                                                              name=trigger_rtf_update_datalake_for_user_id,
                                                              description='Masking data in datalake and Generating masking data script in SQL',
                                                              workflow_name=workflow_rtf_mask_user_data.name,
                                                              type='ON_DEMAND',
                                                              actions=[glue_.CfnTrigger.ActionProperty(
                                                                  job_name=rtf_update_datalake_for_user.name) and
                                                                  glue_.CfnTrigger.ActionProperty(
                                                                      job_name=rtf_get_redshift_sql_update.name)
                                                              ],
                                                              start_on_creation=False,
                                                              )
        trigger_rtf_run_sql_update_script_id = 'dp_trigger_rtf_run_sql_update_script'
        trigger_rtf_run_sql_update_script = glue_.CfnTrigger(self, trigger_rtf_run_sql_update_script_id,
                                                                   name=trigger_rtf_run_sql_update_script_id,
                                                                   description='Running SQL update script to mask',
                                                                   workflow_name=workflow_rtf_mask_user_data.name,
                                                                   type='CONDITIONAL',
                                                                   predicate={'conditions': [
                                                                       glue_.CfnTrigger.ConditionProperty(
                                                                           job_name=rtf_get_redshift_sql_update.name,
                                                                           state='SUCCEEDED',
                                                                           logical_operator='EQUALS')
                                                                   ]},
                                                                   actions=[glue_.CfnTrigger.ActionProperty(
                                                                       job_name=rtf_run_sql_update_script.name)
                                                                   ],
                                                                   start_on_creation=True,
                                                                   )


        
        # Alarm for GlueJob
        topic = sns.Topic(self, id='gluetopic', topic_name='GlueJobTopic')
        try:
            subscription = sns.Subscription(self, id='serialisation_subscription', topic=topic,
                                            protocol=sns.SubscriptionProtocol.EMAIL,
                                            endpoint=stack_params['sns_email_test'])
        except:
            pass
        topic_target = etargets.SnsTopic(topic=topic)
        event_pattern = events.EventPattern(detail={"state": ["FAILED"]}, source=["aws.glue"], detail_type=["Glue Job State Change"])

        glue_rule = events.Rule(self, id='glue_rule', event_pattern=event_pattern, rule_name='GlueJobRule')
        glue_rule.add_target(topic_target)
        print('## GLUE: definition finished  ##')
