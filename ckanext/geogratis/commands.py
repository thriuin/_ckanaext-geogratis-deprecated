from ckan.lib.cli import CkanCommand
from ckanext.canada.metadata_schema import schema_description
from paste.script import command
from time import sleep
import csv
import dateutil.parser
import datetime
import logging
import os.path
import paste.script
import re 
import simplejson as json
import urllib2
import sys

class GeogratisCommand(CkanCommand):
    """
    CKAN Geogratis Extension
    
    Usage:
        paster geogratis print_one -u <uuid> [-f <file-name>] 
                         updated -d <date-time> [-f <file-name>] [-r <report_file>] [-n]
                         get_all [-f <file-name>] [-r <report_file>] [-n]
                         [-h | --help]
                         
    Arguments:
        <uuid>        is the Geogratis dataset ID number
        <date-time>   is datetime string in ISO 8601 format e.g. "2013-01-30T01:30:00"
        <file-name>   is the name of a text file to write out the updated records in JSON Lines format
        <report_file> is the name of a text to write out a import records report in .csv format
        
    Options:
        -n Do not print datasets to file. Helpful for testing.
        -h Display help message
        
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    
    parser = command.Command.standard_parser(verbose=True)           
    parser.add_option('-u', '--uuid', dest='uuid', help='Geogratis dataset ID number')
    parser.add_option('-d', '--date', dest='date', help='datetime string in ISO 8601 format')
    parser.add_option('-r', '--report-file', dest='report_file', help='text to write out a import records report')
    parser.add_option('-f', '--json-file', dest='jl_file', help='imported Geogratis records in JSON lines format')
    parser.add_option('-n', '--no-print', dest='noprint', action='store_true', help='Do not print out the JSON records')
    parser.add_option('-c', '--config', dest='config',
        default='development.ini', help='Configuration file to use.')
                
    def command(self):
        '''
        Parse command line arguments and call appropriate method.
        '''
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print self.__doc__
            return
        
        cmd = self.args[0]

        self._load_config()
        
        self.logger = logging.getLogger('ckanext')
           
        '''
        Create look-up table (dicts) of valid choices from the Open Data schema for the following fields;
        * topic categories
        * resource file format types
        * geographic regions
        '''   
        
        # Topic categories
        self.topic_choices = dict((c['eng'], c)
            for c in schema_description.dataset_field_by_id['topic_category']['choices'] if 'eng' in c)
        
        # Resource file types - additional mappings to the correct types are added 
        # for Geogratis because the file formats in Geogratis do not match one-for-one with Open Data formats
        self.format_types = dict((item['eng'], item['key']) 
            for item in schema_description.resource_field_by_id['format']['choices'])
        self.format_types['GeoTIFF (Georeferenced Tag Image File Format)'] = 'geotif'
        self.format_types['TIFF (Tag Image File Format)'] = "tiff"
        self.format_types['GeoTIFF'] = 'geotif'
        self.format_types['Adobe PDF'] = 'PDF'
        self.format_types['PDF - Portable Document Format'] = "PDF"    
        self.format_types['ASCII (American Standard Code for Information Interchange)'] = "TXT"
        self.format_types['GML (Geography Markup Language)'] = "gml"
        self.format_types['Shape'] = "SHAPE"
        self.format_types['gzip (GNU zip)'] = "ZIP"
        self.format_types['ZIP'] = "ZIP"
        self.format_types['ESRI Shapefile'] = "SHAPE"
        self.format_types['JPEG'] = "jpg"
        self.format_types['Jpeg 2000'] = "jpeg 2000"
                    
        # Geographic regions - note that Open Data uses far fewer regions than Geogratis
        self.geographic_regions= dict ((region['eng'],region['key']) 
            for region in schema_description.dataset_field_by_id['geographic_region']['choices'])
  
        self.presentation_forms = {}    
        self.presentation_forms['documentDigital'] = u"Document Digital | Document num\u00e9rique"
        self.presentation_forms['documentHardcopy'] = u"Document Hardcopy | Document papier"
        self.presentation_forms['imageDigital'] = u"Image Digital | Image num\u00e9rique"
        self.presentation_forms['imageHardcopy'] = u"Image Hardcopy | Image papier"
        self.presentation_forms['mapDigital'] = u"Map Digital | Carte num\u00e9rique"
        self.presentation_forms['mapHardcopy'] = u"Map Hardcopy | Carte papier"
        self.presentation_forms['modelDigital'] = u"Model Digital | Mod\u00e8le num\u00e9rique"
        self.presentation_forms['modelHardcopy'] = u"Model Hardcopy | Maquette"
        self.presentation_forms['profileDigital'] = u"Profile Digital | Profil num\u00e9rique"
        self.presentation_forms['profileHardcopy'] = u"Profile Hardcopy | Profil papier"
        self.presentation_forms['tableDigital'] = u"Table Digital | Table num\u00e9rique"
        self.presentation_forms['tableHardcopy'] = u"Table Hardcopy | Table papier"
        self.presentation_forms['videoDigital'] = u"Video Digital | Vid\u00e9o num\u00e9rique"
        self.presentation_forms['videalHardcopy'] = u"Video Hardcopy | Vid\u00e9o film"
        self.presentation_forms['audioDigital'] = u"Audio Digital | Audio num\u00e9rique"
        self.presentation_forms['audioHardcopy'] = u"Audio Hardcopy | Audio analogique"
        self.presentation_forms['multimediaDigital'] = u"Multimedia Digital | Multim\u00e9dia num\u00e9rique"
        self.presentation_forms['multimediaHardcopy'] = u"Multimedia Hardcopy | Multim\u00e9dia analogique"
        self.presentation_forms['diagramDigial'] = u"Diagram Digital | Diagramme num\u00e9rique"
        self.presentation_forms['diagramHardcopy'] = u"Diagram Hardcopy | Diagramme papier"
        
        self.output_file = sys.stdout
        self.display_formatted = True
        
        # Default output is JSON lines (one JSON record per line) but human-readable formatting is an option
        if self.options.jl_file:
            self.output_file = open(os.path.normpath(self.options.jl_file), 'wt')
            self.display_formatted = False
                       
        '''
        Command: print_one - retrieve one record from Geogratis and print it out.
        '''                
        if cmd == 'print_one':

            json_data = None
            try:
                if not self.options.uuid:
                    print self.__doc__
                    return
                json_obj = self._get_geogratis_item(self.options.uuid, 'en')
                print  >>  self.output_file, json.dumps(json_obj, indent = 2 * ' ')
                
            except urllib2.URLError, e:
                logger.error(e.reason)

        #---------------------
        # Command: updated or get_all - retrieve records from a Geogratis feed and convert to
        #         open data's JSON format.
        
        elif cmd == 'updated' or cmd == 'get_all':
            
            # By default, retrieve all records from Geogratis
            query_string = 'http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst?alt=json'
            
            # Retrieve records using the last-edited date.
            if cmd == 'updated':
                if not self.options.date:
                    print self.__doc__
                    return
                try:
                    dt = dateutil.parser.parse(self.options.date)
                    query_string = 'http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst?edited-min=%s&alt=json' % dt.isoformat()
                except ValueError, e:
                    logger.error('"%s" is an invalid date' % self.options.date)
                    return
      
            # Get the feed from Geogratis. The Atom feed only provides a list of datasets which then need to pulled in
            # one by one in their entirety.
            try:
                json_obj = self._get_feed_json_obj(query_string)
            except urllib2.URLError, e:
                logger.error(e.reason)
                return
            
            dt = datetime.date.today()
          
            # @TODO: Remove this logic
            i = 0  # artificial limit while developing 
            
            # Log all exports to a CSV file
            
            if self.options.report_file:
                reportf = open(self.options.report_file, 'wt') # use 'at' for appending
                fieldnames = ('ID', 'Pass or Fail', 'Title (EN)', 'Title (FR)', 'Summary (EN)', 'Summary (FR)', 
                              'Topic Categories', 'Keywords', 'Published Date', 'Browse Images',
                              'Series (EN)', 'Series (FR)', 'Series Issue (EN)', 'Series Issue (FR)', 'Reason for Failure')
                self.report = csv.DictWriter(reportf, dialect='excel',fieldnames=fieldnames)
                self.report.writerow(dict(zip(fieldnames, fieldnames)))
            
            # Keep reading from the Atom feed until the end is reached
            while True:
                i = i + 1
                products = json_obj['products']
                for product in products:
                    sleep(0.2)  #Not to overwhelm Geogratis
                    self.reasons = ''
                    
                    # Retrieve and test for English record
                    geoproduct_en = self._get_geogratis_item(product['id'], 'en')
                    if not geoproduct_en:
                        self.logger.warn('Unable to retrieve English record for %s' % product['id'])
                        continue
                    
                    # Do not process Canadian Digital Elevation Data
                    if self._get_product_type(geoproduct_en) == "canadian-digital-elevation-data":
                        continue
                    
                    # Retrieve and test for French record
                    geoproduct_fr = self._get_geogratis_item(product['id'], 'fr')
                    if not geoproduct_fr:
                        self.logger.warn('Unable to retrieve French record for %s' % product['id'])
                        self.reasons = "Unable to retrieve French record"
                        if self.options.report_file:
                            self.report.writerow({'ID' : product['id'], 
                            'Pass or Fail' : False, 
                            'Title (EN)' : geoproduct_en['title'].encode('utf-8'), 
                            'Title (FR)' : '', 
                            'Summary (EN)' : '', 
                            'Summary (FR)' : '', 
                            'Topic Categories' : '', 
                            'Keywords' : '', 
                            'Published Date' : '', 
                            'Browse Images' : '',
                            'Series (EN)' : '', 
                            'Series (FR)' : '', 
                            'Series Issue (EN)' : '', 
                            'Series Issue (FR)' : '',
                            'Reason for Failure' : 'Unable to retrieve French record'})
                        continue
                        
                    # Convert the Geogratis English and French dataset records into an Open Data JSON object    
                    odproduct = self._convert_to_od_dataset(geoproduct_en, geoproduct_fr)
                    if odproduct and not self.options.noprint:
                        if self.display_formatted:
                            print  >>  self.output_file, (json.dumps(odproduct, indent = 2 * ' '))
                        else:
                            print  >>  self.output_file, (json.dumps(odproduct, encoding="utf-8"))

                # get the next set from the Atom feed. When the Atom feed is empty, then we are done.
                json_obj = self._get_feed_json_obj(self._get_next_link(json_obj))
                if json_obj['count'] == 0 or i == 200:
                    break
                
    """
    The following NRCAN fields are mandatory for the Open Data schema:
    * id
    * title (English and French)
    * summary (English and French)form.strip(';')
    * subject
    * topicCategories
    * keywords
    * spatial
    * date_published 
    * browse_graphic_url
    * 
    """
    def _convert_to_od_dataset(self, geoproduct_en, geoproduct_fr):
        
        odproduct = {}
        valid = True
        
        # Boilerplate fields for the Open Data record
        odproduct['id'] = geoproduct_en['id']
        odproduct['author_email'] = "open-ouvert@tbs-sct.gc.ca"
        odproduct['language'] = "eng; CAN | fra; CAN"
        odproduct['owner_org'] = "nrcan-rncan"
        odproduct['department_number'] = "115"
        odproduct['title'] = geoproduct_en['title']
        if len(odproduct['title']) == 0:
            self.reasons = '%s No English Title Given;' % self.reasons
            valid = False
        odproduct['title_fra'] = geoproduct_fr['title']
        if len(odproduct['title_fra']) == 0: 
            self.reasons = '%s No French Title Given;' % self.reasons
            valid = False
        odproduct['notes'] = geoproduct_en.get('summary', 'No title provided')
        odproduct['notes_fra'] = geoproduct_fr.get('summary', u'Pas de titre pr\u00e9vu')
        odproduct['catalog_type'] = u"Geo Data | G\u00e9o"
        odproduct['license_id'] = u"ca-ogl-lgo"
        odproduct['attribution'] = u"Contains information licensed under the Open Government Licence \u2013 Canada."
        odproduct['attribution_fra'] = u"Contient des informations autoris\u00e9es sous la Licence du gouvernement ouvert- Canada"
        
        # The subject and category fields are derived from the topicCategories field in Geogratis.
        # In the CKAN Canada metadata_schema intergace, there is a mapping that determine which GoC subject
        # to use based on the topicCategories being used.
        topics_subjects = self._get_gc_subject_category(geoproduct_en)
        
        odproduct['subject'] = topics_subjects['subjects']
        if len(odproduct['subject']) == 0:
            valid = False
            self.reasons = '%s No GC Subjects;' % self.reasons
        
        odproduct['topic_category'] = topics_subjects['topics']
        if len(odproduct['topic_category']) == 0:
            valid = False
            self.reasons = '%s No GC Topics;' % self.reasons
                    
        # Keywords (Mandatory)
        odproduct['keywords'] = self._extract_keywords(geoproduct_en.get('keywords', []))
        if len(odproduct['keywords']) == 0:
            valid = False
            self.reasons = '%s Missing English Keywords;' % self.reasons
        
        odproduct['keywords_fra'] = self._extract_keywords(geoproduct_fr.get('keywords', []))
        if len(odproduct['keywords_fra']) == 0:
            valid = False        
            self.reasons = '%s Missing English Keywords;' % self.reasons
                
        odproduct['geographic_region'] = self._get_places(geoproduct_en)
        
        odproduct['spatial'] = str(geoproduct_en['geometry']).replace("'", '\"')
        
        try:
            odproduct['date_published'] = geoproduct_en['citation']['publicationDate']
        except:
            odproduct['date_published'] = ''
            valid = False
            self.reasons = '%s Missing Date Published;' % self.reasons
            
        odproduct['spatial_representation_type'] = "Vector | Vecteur"
        
        try:
            for form in geoproduct_en['citation']['presentationForm'].split():

                if form.strip(';') in self.presentation_forms:
                    odproduct['presentation_form'] = self.presentation_forms[form.strip(';')]
        except:
            valid = False
            self.reasons = '%s Missing or invalid Presentation Form;' % self.reasons
        
        try:
            odproduct['browse_graphic_url'] =  geoproduct_en['browseImages'][0]['link']
        except:
            odproduct['browse_graphic_url'] =  "/static/img/canada_default.png"
        odproduct['date_modified'] = geoproduct_en.get('updatedDate', '2000-01-01')
        
        odproduct['maintenance_and_update_frequency'] = "As Needed | Au besoin"
        
        try:
            odproduct['data_series_name'] = geoproduct_en['citation']['series']
        except:
            odproduct['data_series_name'] = ''
            
        try:
            odproduct['data_series_name_fra'] = geoproduct_fr['citation']['series']
        except:
            odproduct['data_series_name_fra'] = '' 
        
        try:
            odproduct['data_series_issue_identification'] = geoproduct_en['citation']['seriesIssue']
        except:
            odproduct['data_series_issue_identification'] = ''
        
        try:
            odproduct['data_series_issue_identification_fra'] = geoproduct_fr['citation']['seriesIssue']
        except:
            odproduct['data_series_issue_identification_fra'] = ''
        
        # This is not a mandatory field
        try:
            odproduct['digital_object_identifier'] = geoproduct_en['citation']['otherCitationDetails']
        except:
            odproduct['digital_object_identifier'] = ""
        
        # Time period coverage is not being set for Geogratis at this time
        odproduct['time_period_coverage_start'] = ""
        odproduct['time_period_coverage_end'] = ""
        
        odproduct['url'] = geoproduct_en['url']
        odproduct['url_fra'] = geoproduct_fr['url']
        
        odproduct['endpoint_url'] = "http://geogratis.gc.ca/api/en"
        odproduct['endpoint_url_fra'] = "http://geogratis.gc.ca/api/fr"
        
        odproduct['ready_to_publish'] = True
        
        odproduct['portal_release_date'] = ""
        
        # Load the resources
        
        try:
            i = 0
            ckan_resources = []
            for resourcefile in geoproduct_en['files']:
                ckan_resource = {}
                ckan_resource['name'] = resourcefile['description']
                ckan_resource['name_fra'] =geoproduct_fr['files'][i]['description']
                ckan_resource['resource_type'] = 'file'
                ckan_resource['url'] = resourcefile['link']
                ckan_resource['size'] = self._to_byte_string(resourcefile['size'])
                ckan_resource['format'] = self._to_format_type(resourcefile['type'])
                ckan_resource['language'] = 'eng; CAN | fra; CAN'
                ckan_resources.append(ckan_resource)
                i += 1
        except:
            valid = False
            self.reasons = '%s No resources;' % self.reasons

        odproduct['resources'] = ckan_resources
     
        # Keep a report of the results
        if self.options.report_file:
            self.report.writerow({'ID' : odproduct['id'], 
                            'Pass or Fail' : valid, 
                            'Title (EN)' : odproduct['title'].encode('utf-8'), 
                            'Title (FR)' : odproduct['title_fra'].encode('utf-8'), 
                            'Summary (EN)' : 'Y' if odproduct['notes'] <> 'No title provided' else 'N', 
                            'Summary (FR)' : 'Y' if odproduct['notes_fra'] <> 'Pas de titre pr\u00e9vu' else 'N', 
                            'Topic Categories' : 'Y' if len(odproduct['topic_category']) > 0 else 'N', 
                            'Keywords' : 'Y' if len(odproduct['topic_category']) > 0 else 'N', 
                            'Published Date' : 'Y' if odproduct['date_published'] <> '' else 'N', 
                            'Browse Images' : 'Y' if odproduct['browse_graphic_url'] <> "/static/img/canada_default.png" else 'N',
                            'Series (EN)' : 'Y' if odproduct['data_series_name'] <> '' else 'N', 
                            'Series (FR)' : 'Y' if odproduct['data_series_name_fra'] <> '' else 'N', 
                            'Series Issue (EN)' : 'Y' if odproduct['data_series_issue_identification'] <> '' else 'N', 
                            'Series Issue (FR)' : 'Y' if odproduct['data_series_issue_identification_fra'] <> '' else 'N',
                            'Reason for Failure' : self.reasons})
        if not valid:
            odproduct = None
        return odproduct

    def _get_feed_json_obj(self, link):
        try:
            response = urllib2.urlopen(link)
            json_data = response.read()
            json_obj = json.loads(json_data)
            json_obj['url'] = link
            return json_obj
        except urllib2.HTTPError, e:
            self.logger.error(e.msg)
        return None

    def _get_geogratis_item(self, geo_id, lang):
        json_obj = self._get_feed_json_obj('http://geogratis.gc.ca/api/%s/nrcan-rncan/ess-sst/%s.json' % (lang, geo_id))
        return json_obj

    def _get_next_link(self, json_obj):
        links = json_obj['links']
        for link in links:
            if link['rel'] == 'next':
                return link['href']
    
    # Obtain a string with comma-separated keywords. For some NRCAN products it is necessary to
    # strip away keyword hierarchy: e.g. for "one > two > three" should only be "three".
    def _extract_keywords(self, keywords):
        simple_keywords = []
        for keyword in keywords:
            words = keyword.split('>')
            if len(words) > 0:
                last_word = words.pop()
                last_word = last_word.strip().replace("/", " - ")
                last_word = last_word.replace("(", "- ").replace(")", "") # change "one (two)" to "one - two"
                simple_keywords.append(last_word)
        return ','.join(simple_keywords)
            
    def _get_product_type(self, geoproduct):
        product_type = ""
        categories = geoproduct['categories']
        for category in categories:
          if category['type'] == 'urn:iso:series':
              product_type = category['terms'][0]['term']
      
    # Return the first match for a geographic region. Note that for the region 'Canada' the value is
    # and empty string since this assumed to be the default
    def _get_places(self, geoproduct_en):
        places = ""
        for category in geoproduct_en['categories']:
            if category['type'] == 'urn:iso:place':
                for term in category['terms']:
                    if term["label"] in self.geographic_regions.keys() and term["label"] <> "Canada":
                        places = self.geographic_regions[term["label"]]
                        break
                break            
        return places
        
    '''
    The Open Data schema uses the Government of Canada (GoC) thesaurus to enumerate valid topics and subjects.
    The schema provides a mapping of subjects to topic categories. Geogratis records provide GoC topics.
    This function looks up the subjects for these topics and returns two dictionaries with appropriate 
    Open Data topics and subjects for this Geogratis record.
    '''
    def _get_gc_subject_category(self, geoproduct_en):
        topics = []
        subjects = []

        schema_categories = schema_description.dataset_field_by_id['topic_category']['choices']
        topic_categories = geoproduct_en.get('topicCategories', [])
        
        # Subjects are mapped to the topics in the schema, so both are looked up from the topic keys
        for topic in topic_categories:
            topic_key = re.sub("([a-z])([A-Z])","\g<1> \g<2>", topic).title()
            topics.append(self.topic_choices[topic_key]['key'])
            topic_subject_keys = self.topic_choices[topic_key]['subject_ids']
            for topic_subject_key in topic_subject_keys:
                subjects.append(schema_description.dataset_field_by_id['subject']['choices_by_id'][topic_subject_key]['key'])

        return { 'topics' : topics, 'subjects' : subjects}
    
    def _encapsulate_geojson(self,geodict):
        new_geodict = {}
        for key in geodict.keys():
            new_key ='"%s"' % (key,)
            if isinstance(geodict[key], basestring):
                new_geodict[new_key] = '"%s"' % (geodict[key],)
            else:
                new_geodict[new_key] = geodict[key]
        return new_geodict
    
    # Take a Geogratis file size string (e.g. 1.25 MB) and convert into a number of bytes in base 10l
    def _to_byte_string(self, filesize):
        parts = filesize.split()
        num = float(parts[0])
        if parts[1] == 'KB':
            num = num * 1024
        elif parts[1] == 'MB':
            num = num * 1048576
        elif parts[1] == 'GB':
            num = num * 1073741824
        else:
            num = 0
        return int(round(num, -1))
    
    def _to_format_type(self, geogratis_type):
        if geogratis_type not in self.format_types:
            return 'Other'
        else:
            return self.format_types[geogratis_type]        
        
  