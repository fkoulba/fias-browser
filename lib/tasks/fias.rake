# encoding: UTF-8

namespace :fias do
  desc 'Import data from FIAS XML to database'
  task :import_from_xml => :environment do
    connection = ActiveRecord::Base.connection

    class FiasXmlDocument < Nokogiri::XML::SAX::Document
      attr_accessor :connection, :table, :create_query, :index_query, :columns, :node

      def start_document
        @objects = []
        @parsed = 0

        connection.execute("DROP TABLE IF EXISTS #{table}")

        connection.execute(create_query)

        puts "Table #{table} created"
      end

      def end_document
        flush_objects

        connection.execute(index_query)

        puts "Indexes for #{table} created"
      end

      def start_element(name, attrs = [])
        return if name != node

        object = Hash[*attrs.flatten(1)]
        @objects << object

        flush_objects if @objects.size >= 1000
      end

      def flush_objects
        return if @objects.size <= 0

        # @objects.each do |object|
        #   connection.execute("INSERT INTO #{table} (#{columns.keys.collect {|e| connection.quote_column_name(e) }.join(', ')}) VALUES (#{columns.collect {|field, type| connection.quote((object[field].tap {|v| case type when :string then v.to_s when :integer then v.to_i when :date then v.to_date end } rescue nil)) }.join(', ') })")
        # end

        # connection.execute("INSERT INTO #{table} (#{columns.keys.collect {|e| connection.quote_column_name(e) }.join(', ')}) #{@objects.collect {|row| "SELECT #{columns.collect {|field, type| connection.quote((row[field].tap {|v| case type when :string then v.to_s when :integer then v.to_i when :date then v.to_date end } rescue nil)) }.join(', ') }" }.join(" UNION ") }")

        connection.execute("INSERT INTO #{table} (#{columns.keys.collect {|e| connection.quote_column_name(e) }.join(', ')}) VALUES #{@objects.collect {|row| "(#{columns.collect {|field, type| connection.quote((row[field].tap {|v| case type when :string then v.to_s when :integer then v.to_i when :date then v.to_date end } rescue nil)) }.join(', ') })" }.join(', ') }")


        @parsed += @objects.size

        @objects.clear

        puts "Parsed #{@parsed} records into #{table}"
      end
    end

    fias_document_types = {
      :addrobj => {
        :table => '`fias_addrobj`',
        :columns => {
          'AOGUID' => :string,
          'FORMALNAME' => :string,
          'REGIONCODE' => :string,
          'AUTOCODE' => :string,
          'AREACODE' => :string,
          'CITYCODE' => :string,
          'CTARCODE' => :string,
          'PLACECODE' => :string,
          'STREETCODE' => :string,
          'EXTRCODE' => :string,
          'SEXTCODE' => :string,
          'OFFNAME' => :string,
          'POSTALCODE' => :string,
          'IFNSFL' => :string,
          'TERRIFNSFL' => :string,
          'IFNSUL' => :string,
          'TERRIFNSUL' => :string,
          'OKATO' => :string,
          'OKTMO' => :string,
          'UPDATEDATE' => :date,
          'SHORTNAME' => :string,
          'AOLEVEL' => :integer,
          'PARENTGUID' => :string,
          'AOID' => :string,
          'PREVID' => :string,
          'NEXTID' => :string,
          'CODE' => :string,
          'PLAINCODE' => :string,
          'ACTSTATUS' => :integer,
          'CENTSTATUS' => :integer,
          'OPERSTATUS' => :integer,
          'CURRSTATUS' => :integer,
          'STARTDATE' => :date,
          'ENDDATE' => :date,
          'NORMDOC' => :string
        },
        :create_query => "CREATE TABLE `fias_addrobj` (
          `AOGUID` varchar(36) DEFAULT NULL COMMENT 'Глобальный уникальный идентификатор адресного объекта',
          `FORMALNAME` varchar(120) DEFAULT NULL COMMENT 'Формализованное наименование',
          `REGIONCODE` varchar(2) DEFAULT NULL COMMENT 'Код региона',
          `AUTOCODE` varchar(1) DEFAULT NULL COMMENT 'Код автономии',
          `AREACODE` varchar(3) DEFAULT NULL COMMENT 'Код района',
          `CITYCODE` varchar(3) DEFAULT NULL COMMENT 'Код города',
          `CTARCODE` varchar(3) DEFAULT NULL COMMENT 'Код внутригородского района',
          `PLACECODE` varchar(3) DEFAULT NULL COMMENT 'Код населенного пункта',
          `STREETCODE` varchar(4) DEFAULT NULL COMMENT 'Код улицы',
          `EXTRCODE` varchar(4) DEFAULT NULL COMMENT 'Код дополнительного адресообразующего элемента',
          `SEXTCODE` varchar(3) DEFAULT NULL COMMENT 'Код подчиненного дополнительного адресообразующего элемента',
          `OFFNAME` varchar(120) DEFAULT NULL COMMENT 'Официальное наименование',
          `POSTALCODE` varchar(6) DEFAULT NULL COMMENT 'Почтовый индекс',
          `IFNSFL` varchar(4) DEFAULT NULL COMMENT 'Код ИФНС ФЛ',
          `TERRIFNSFL` varchar(4) DEFAULT NULL COMMENT 'Код территориального участка ИФНС ФЛ',
          `IFNSUL` varchar(4) DEFAULT NULL COMMENT 'Код ИФНС ЮЛ',
          `TERRIFNSUL` varchar(4) DEFAULT NULL COMMENT 'Код территориального участка ИФНС ЮЛ',
          `OKATO` varchar(11) DEFAULT NULL COMMENT 'ОКАТО',
          `OKTMO` varchar(11) DEFAULT NULL COMMENT 'ОКТМО',
          `UPDATEDATE` date DEFAULT NULL COMMENT 'Дата внесения (обновления) записи',
          `SHORTNAME` varchar(10) DEFAULT NULL COMMENT 'Краткое наименование типа объекта',
          `AOLEVEL` int(10) DEFAULT NULL COMMENT 'Уровень адресного объекта',
          `PARENTGUID` varchar(36) DEFAULT NULL COMMENT 'Идентификатор объекта родительского объекта',
          `AOID` varchar(36) DEFAULT NULL COMMENT 'Уникальный идентификатор записи. Ключевое поле.',
          `PREVID` varchar(36) DEFAULT NULL COMMENT 'Идентификатор записи связывания с предыдушей исторической записью',
          `NEXTID` varchar(36) DEFAULT NULL COMMENT 'Идентификатор записи связывания с последующей исторической записью',
          `CODE` varchar(17) DEFAULT NULL COMMENT 'Код адресного объекта одной строкой с признаком актуальности из КЛАДР 4.0.',
          `PLAINCODE` varchar(15) DEFAULT NULL COMMENT 'Код адресного объекта из КЛАДР 4.0 одной строкой без признака актуальности (последних двух цифр)',
          `ACTSTATUS` int(10) DEFAULT NULL COMMENT 'Статус актуальности адресного объекта ФИАС. Актуальный адрес на текущую дату. Обычно последняя запись об адресном объекте. 0 – Не актуальный 1 - Актуальный',
          `CENTSTATUS` int(10) DEFAULT NULL COMMENT 'Статус центра',
          `OPERSTATUS` int(10) DEFAULT NULL COMMENT 'Статус действия над записью – причина появления записи (см. описание таблицы OperationStatus): 01 – Инициация; 10 – Добавление; 20 – Изменение; 21 – Групповое изменение; 30 – Удаление; 31 - Удаление вследствие удаления вышестоящего объекта; 40 – Присоединение адресного объекта (слияние); 41 – Переподчинение вследствие слияния вышестоящего объекта; 42 - Прекращение существования вследствие присоединения к другому адресному объекту; 43 - Создание нового адресного объекта в результате слияния адресных объектов; 50 – Переподчинение; 51 – Переподчинение вследствие переподчинения вышестоящего объекта; 60 – Прекращение существования вследствие дробления; 61 – Создание нового адресного объекта в результате дробления; 70 – Восстановление прекратившего существование объекта',
          `CURRSTATUS` int(10) DEFAULT NULL COMMENT 'Статус актуальности КЛАДР 4 (последние две цифры в коде)',
          `STARTDATE` date DEFAULT NULL COMMENT 'Начало действия записи',
          `ENDDATE` date DEFAULT NULL COMMENT 'Окончание действия записи',
          `NORMDOC` varchar(36) DEFAULT NULL COMMENT 'Внешний ключ на нормативный документ'
        )",
        :index_query => "ALTER TABLE `fias_addrobj`
          ADD INDEX `AOID` (`AOID`),
          ADD INDEX `AOGUID` (`AOGUID`),
          ADD INDEX `PARENTGUID` (`PARENTGUID`),
          ADD INDEX `ACTSTATUS` (`ACTSTATUS`),
          ADD INDEX `CURRSTATUS` (`CURRSTATUS`),
          ADD INDEX `OFFNAME` (`OFFNAME`),
          ADD INDEX `SHORTNAME` (`SHORTNAME`)
        ",
        :filename => 'addrobj.xml',
        :node => 'Object'
      },
      :house => {
        :table => '`fias_house`',
        :columns => {
          'POSTALCODE' => :string,
          'IFNSFL' => :string,
          'TERRIFNSFL' => :string,
          'IFNSUL' => :string,
          'TERRIFNSUL' => :string,
          'OKATO' => :string,
          'OKTMO' => :string,
          'UPDATEDATE' => :datetime,
          'HOUSENUM' => :string,
          'ESTSTATUS' => :integer,
          'BUILDNUM' => :string,
          'STRUCNUM' => :string,
          'STRSTATUS' => :integer,
          'HOUSEID' => :string,
          'HOUSEGUID' => :string,
          'AOGUID' => :string,
          'STARTDATE' => :date,
          'ENDDATE' => :date,
          'STATSTATUS' => :integer,
          'NORMDOC' => :string,
          'COUNTER' => :integer
        },
        :create_query => "CREATE TABLE `fias_house` (
          `POSTALCODE` VARCHAR(6) NULL DEFAULT NULL COMMENT 'Почтовый индекс',
          `IFNSFL` VARCHAR(4) NULL DEFAULT NULL COMMENT 'Код ИФНС ФЛ',
          `TERRIFNSFL` VARCHAR(4) NULL DEFAULT NULL COMMENT 'Код территориального участка ИФНС ФЛ',
          `IFNSUL` VARCHAR(4) NULL DEFAULT NULL COMMENT 'Код ИФНС ЮЛ',
          `TERRIFNSUL` VARCHAR(4) NULL DEFAULT NULL COMMENT 'Код территориального участка ИФНС ЮЛ',
          `OKATO` VARCHAR(11) NULL DEFAULT NULL COMMENT 'ОКАТО',
          `OKTMO` VARCHAR(11) NULL DEFAULT NULL COMMENT 'ОК TMO',
          `UPDATEDATE` DATETIME NULL DEFAULT NULL COMMENT 'Дата время внесения (обновления) записи',
          `HOUSENUM` VARCHAR(20) NULL DEFAULT NULL COMMENT 'Номер дома',
          `ESTSTATUS` INT(1) NULL DEFAULT NULL COMMENT 'Признак владения',
          `BUILDNUM` VARCHAR(10) NULL DEFAULT NULL COMMENT 'Номер корпуса',
          `STRUCNUM` VARCHAR(10) NULL DEFAULT NULL COMMENT 'Номер строения',
          `STRSTATUS` INT(10) NULL DEFAULT NULL COMMENT 'Признак строения',
          `HOUSEID` VARCHAR(36) NULL DEFAULT NULL COMMENT 'Уникальный идентификатор записи дома',
          `HOUSEGUID` VARCHAR(36) NULL DEFAULT NULL COMMENT 'Глобальный уникальный идентификатор дома',
          `AOGUID` VARCHAR(36) NULL DEFAULT NULL COMMENT 'Guid записи родительского объекта (улицы, города, населенного пункта и т.п.)',
          `STARTDATE` DATE NULL DEFAULT NULL COMMENT 'Начало действия записи',
          `ENDDATE` DATE NULL DEFAULT NULL COMMENT 'Окончание действия записи',
          `STATSTATUS` INT(11) NULL DEFAULT NULL COMMENT 'Состояние дома',
          `NORMDOC` VARCHAR(36) NULL DEFAULT NULL COMMENT 'Внешний ключ на нормативный документ',
          `COUNTER` INT(11) NULL DEFAULT NULL COMMENT 'Счетчик записей домов для КЛАДР 4'
        )",
        :index_query => "ALTER TABLE `fias_house`
          ADD INDEX `HOUSEGUID` (`HOUSEGUID`),
          ADD INDEX `AOGUID` (`AOGUID`),
          ADD INDEX `STRSTATUS` (`STRSTATUS`),
          ADD INDEX `STARTDATE` (`STARTDATE`),
          ADD INDEX `ENDDATE` (`ENDDATE`)
        ",
        :filename => 'house.xml',
        :node => 'House'
      },
      :socrbase => {
        :table => '`fias_socrbase`',
        :columns => {
          'LEVEL' => :integer,
          'SCNAME' => :string,
          'SOCRNAME' => :string,
          'KOD_T_ST' => :string,
        },
        :create_query => "CREATE TABLE `fias_socrbase` (
          `LEVEL` INT(10) NULL DEFAULT NULL COMMENT 'Уровень адресного объекта',
          `SCNAME` VARCHAR(10) NULL DEFAULT NULL COMMENT 'Краткое наименование типа объекта',
          `SOCRNAME` VARCHAR(50) NULL DEFAULT NULL COMMENT 'Полное наименование типа объекта',
          `KOD_T_ST` VARCHAR(4) NULL DEFAULT NULL COMMENT 'Ключевое поле'
        )",
        :index_query => "ALTER TABLE `fias_socrbase`
          ADD INDEX `KOD_T_ST` (`KOD_T_ST`),
          ADD INDEX `LEVEL` (`LEVEL`),
          ADD INDEX `SCNAME` (`SCNAME`),
          ADD INDEX `SOCRNAME` (`SOCRNAME`)
        ",
        :filename => 'socrbase.xml',
        :node => 'AddressObjectType'
      }
    }

    [:addrobj, :house, :socrbase].collect {|type| fias_document_types[type] }.each do |props|
      document = FiasXmlDocument.new
      document.connection = connection
      document.table = props[:table]
      document.create_query = props[:create_query].gsub(/( COMMENT '.*')?/i, '')
      document.columns = props[:columns]
      document.index_query = props[:index_query]
      document.node = props[:node]

      parser = Nokogiri::XML::SAX::Parser.new(document)
      parser.parse(File.open(Rails.root.join('data', 'fias', props[:filename]), 'r'))
    end
  end
end
