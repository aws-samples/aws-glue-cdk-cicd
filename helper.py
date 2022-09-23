# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import tempfile
import shutil
from pathlib import Path


def create_archive(zip_name='glue_blog'):
    root_dir = Path(os.path.dirname(os.path.abspath(__file__))).absolute()

    # Add additional ignore files/directories within the ignore argument
    with tempfile.TemporaryDirectory() as tmpdir:
        shutil.copytree(
            root_dir,
            os.path.join(tmpdir, 'glue_blog'),
            ignore=shutil.ignore_patterns(
                '__pycache__',
                'cdk.out',
                '.git',
                '.DS_Store',
                '.venv',
                'node_modules',
                'logs',
                '.pytest_cache',
                '.tox',
                'htmlcov',
                '.coverage',
                'coverage.xml',
                'junitxml.xml'
            )
        )

        shutil.make_archive(
            os.path.join('cdk.out/', zip_name),
            'zip',
            os.path.join(tmpdir, 'glue_blog')
        )

    return os.path.join('cdk.out/', zip_name+".zip")
