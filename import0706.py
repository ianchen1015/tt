import csv
from course.models import CourseCategory, Course, Grade, Unit, Subject, TeachingMaterial, ResourceType
from django.contrib.auth.models import User
from community.models import Community
from django.utils.text import slugify
from course.tasks import log_course_view, log_material_download, convert_material_filetype

import os
import requests
import urllib

from botocore.exceptions import ClientError
from course.models import TeachingMaterial
from s3.utils import S3, COURSE_MATERIAL_BUCKET


# GDrive file download
def download_file_from_google_drive(id, destination):
    URL = "https://docs.google.com/uc?export=download"
    session = requests.Session()

    response = session.get(URL, params = { "id" : id }, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { "id" : id, "confirm" : token }
        response = session.get(URL, params = params, stream = True)

    print(response.headers)
    encoded_fname = response.headers["Content-Disposition"].split(";filename*=UTF-8''")[-1]
    filename = urllib.parse.unquote(encoded_fname).encode().decode()
    full_file_path = destination + filename
    save_response_content(response, full_file_path)
    return filename

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            return value

    return None

def save_response_content(response, full_file_path):
    CHUNK_SIZE = 32768

    with open(full_file_path, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

def check_file_exist(s3, bucket, key):
    try:
        s3.client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        return int(e.response['Error']['Code']) != 404
    return True

# End of GDrive file download



def create_course(row):
    ctitle = row[1]
    cintro = ''
    cat_list = CourseCategory.objects.filter(level=2).filter(name='數學')
    grades = []
    units = []
    for grade_num in row[2].split(','):
        grade = Grade.objects.get(grade=grade_num)
        grades.append(grade)
    for sub_unit in row[3].split(','):
        if '/' in sub_unit:
            subject_name = sub_unit.split('/')[0]
            unit_name = sub_unit.split('/')[1]
        else:
            subject_name = sub_unit
            unit_name = None
            
        subject = Subject.objects.get(name=subject_name)
        unit, created = Unit.objects.get_or_create(
            subject=subject,
            name=unit_name,
            slug = slugify(unit_name, allow_unicode=True),
        )
        units.append(unit)

    print("course title: ", ctitle)
    print("course intro: ", cintro)
    print("cat list: ", cat_list)
    print("grades: ", grades)
    print("units: ", units)

    
    course = Course.objects.create(
        owner=user,
        title=ctitle,
        introduction=cintro,
        community=community,
        privacy_level=3
    )
    for cat in cat_list:
        if cat:
            course.category.add(cat)
    
    for grade in grades:
        course.grades.add(grade)
    
    for unit in units:
        course.units.add(unit)
    
    return course
    


def create_material(row, cur_course, material_name, serial_no):


    if row[4]:# G doc
        mname = material_name
        mintro = ''
        mime_type = mime_type_dict[row[5]]
        #serial_no = 1
        if "open?id=" in row[4]:
            gdrive_id = row[4].split("open?id=")[-1]
        else:
            gdrive_id = row[4].split("/")[-2]
        
        print("material name: ", mname)
        print("material intro: ", mintro)
        print("material mime type: ", mime_type)
        print("serial_no: ", serial_no)
        print("gdrive_id: ", gdrive_id)
        
        material = TeachingMaterial.objects.create(
            material_name=mname,
            owner=user,
            subordinate_course=cur_course,
            introduction=mintro,
            originated=True,
            content_source="",
            content_type=mime_type,
            serial_in_course=serial_no,
            gdrive_file_id=gdrive_id
        )
        
        return material

    else:# youtube
        mname = material_name
        mintro = ''
        #serial_no = 1
        print("material name: ", mname)
        print("material intro: ", mintro)
        print("serial_no: ", serial_no)
        print("material_url: ", row[6])

        material = TeachingMaterial.objects.create(
            material_name=mname,
            owner=user,
            subordinate_course=cur_course,
            introduction=mintro,
            originated=False,
            content_source="",
            #content_type=mime_type,
            serial_in_course=serial_no,
            #gdrive_file_id=gdrive_id
            material_url=row[6],
            material_type=2,
            #source_type=1
        )
        
        return material

mime_type_dict = {
    "word": "application/msword",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "pdf": "application/pdf",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".pdf": "application/pdf",
    "ppt": "application/vnd.ms-powerpoint",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "gif": "image/gif",
}


'''
user = User.objects.get(pk=1540) #數咖管理員管理員 in prod db
community = Community.objects.get(pk=5) #數咖管理員 in prod db
'''
user = User.objects.get(pk=1) #test in prod db
community = Community.objects.get(pk=1) #test in prod db


destination = os.getcwd() + "/manage_script/import_mathcafe_resources/"
s3 = S3()

with open('./manage_script/import_mathcafe_resources/output0706.tsv') as f:
    reader = csv.reader(f, delimiter='\t')
    for index, row in enumerate(reader):

        if index not in [4,5]:#1,4,5,18]:
            continue

        print("\n\n\n\n\n\n\n")
        print('\n', index, '/',  '\n')
        print('+++++',row[1])
        
        if index == 0:
            continue

        if row[3]:# if it is the head of course
            # create course & material
            print("==========")
            print("creating course: {}".format(row[1]))
            course = create_course(row)
            material_name = row[1]
            serial_no = 1# serial_no start from 1

        # for every material
        print("---")
        print("creating material: {}".format(row[1]))
        material = create_material(row, course, material_name, serial_no)
        serial_no += 1
        course.save()
        
        # end of create course & material

        if row[4]:
            # download file from google drive
            print("downloading from Google Drive: %s - %s" % (material.id, material))
            current_filename = download_file_from_google_drive(material.gdrive_file_id, destination)
            current_file_full_path = destination + current_filename

            # get uuid prefix
            on_wait_material_id = s3.generate_unique_key()
            key = "{id}_{filename}".format(id=on_wait_material_id, filename=current_filename)

            # upload to s3
            print("uploading to s3: %s" % key)
            resp = s3.client.upload_file(current_file_full_path, COURSE_MATERIAL_BUCKET, key, ExtraArgs={"ContentType": material.content_type})

            print('check whether the file is in the bucket')
            while not check_file_exist(s3, COURSE_MATERIAL_BUCKET, key):
                print('checking again...')
                check_file_exist(s3, COURSE_MATERIAL_BUCKET, key)
            print('done checking, the file is in the bucket')

            # save s3 key to materials
            print("updating to DB: %s" % material)
            material.aws_bucket = COURSE_MATERIAL_BUCKET
            material.aws_key = key
            material.save()

            # delete tmp file
            print("removing local file %s" % key)
            os.remove(current_file_full_path)

            print("file transfering has done successfully: %s \n\n\n" % material)

            # convert file to pdf and parse fulltext
            convert_material_filetype.delay(material.material_id)
        else:
            material.save()