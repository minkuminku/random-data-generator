package com.mayank.s3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

public class RandomDataGenerator {

    private static final DateTimeFormatter ISO8601_FORMATFORFILE = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
    private static final int MAX_FILE_SIZE_MB = 100;
    private static final int MAX_FILES = 256; // gives 25 GB files


    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.###");
    private static final DateTimeFormatter ISO8601_FORMAT = DateTimeFormatter.ISO_DATE_TIME;


    private static final String[] INSURANCE_COMPANIES = {
            "AIA Group", "AIG", "Allianz", "Allstate", "American Family Insurance",
            "American National Insurance Company", "Amica Mutual Insurance", "Anthem",
            "Assicurazioni Generali", "Aviva", "AXA", "Bankers Life", "Berkshire Hathaway",
            "Chubb Limited", "Cigna", "CNA Financial", "Colonial Life Accident Insurance Company",
            "Country Financial", "Erie Insurance", "Esurance", "Farmers Insurance Group", "GEICO",
            "Gen Re", "Genworth Financial", "Great Eastern Life", "Great West Life Assurance Company",
            "Guardian Life Insurance Company of America", "Hartford Financial Services Group", "Hiscox",
            "ICICI Lombard", "ING Group", "Jackson National Life", "John Hancock Financial",
            "Legal General", "Liberty Mutual", "Lincoln National Corporation", "Lloyds of London",
            "Manulife Financial", "MassMutual", "MetLife", "Munich Re", "National General Insurance",
            "Nationwide Mutual Insurance Company", "New York Life Insurance Company", "Northwestern Mutual",
            "Ohio National Financial Services", "Old Mutual", "Pacific Life", "Ping An Insurance",
            "Principal Financial Group", "Progressive", "Prudential Financial", "Prudential plc",
            "QBE Insurance", "RGA Reinsurance Company", "Royal Sun Alliance", "Securian Financial Group",
            "Standard Life", "State Farm", "Sun Life Financial", "Swiss Re", "TIAA-CREF",
            "The Hanover Insurance Group", "The Hartford", "The Phoenix Companies", "The Standard",
            "Tokio Marine HCC", "Travelers", "Unum", "USAA", "Voya Financial",
            "Western  Southern Financial Group", "XL Catlin", "Zurich Insurance Group", "Affiliated FM Insurance",
            "Alfa Insurance", "American Strategic Insurance", "Ameriprise Auto  Home Insurance",
            "Anchor General Insurance", "Arbella Insurance Group", "Auto-Owners Insurance",
            "Brethren Mutual Insurance Company", "Bristol West Insurance Group", "California Casualty",
            "Central Insurance Companies", "Cincinnati Insurance Company", "Citizens Property Insurance Corporation",
            "Clearcover", "Commerce Insurance Group", "Concord Group Insurance", "COUNTRY Financial",
            "CSAA Insurance Group", "Dairyland Insurance", "Direct Auto Insurance", "Donegal Insurance Group",
            "Elephant Insurance", "EMC Insurance Companies", "Encompass Insurance", "Erie Insurance Group",
            "Everest Re", "Farm Bureau Insurance of Tennessee", "Foremost Insurance Group",
            "Frankenmuth Insurance", "Freedom National Insurance Services", "Gainsco", "Germania Insurance",
            "Good2Go Auto Insurance", "Grange Insurance", "Hanover Insurance", "The Hartford", "Horace Mann",
            "Indiana Farm Bureau Insurance", "Infinity Insurance", "Kemper Corporation", "Kentucky Farm Bureau",
            "Liberty Mutual Insurance", "Markel Corporation", "Mercury Insurance Group", "Metromile",
            "Mississippi Farm Bureau Insurance", "National General Insurance", "Nationwide Mutual Insurance Company",
            "NJM Insurance Group", "Nodak Mutual Insurance Company", "Ohio Mutual Insurance Group",
            "Palisades Safety and Insurance Association", "PEAK6 Investments", "PEMCO Insurance",
            "Plymouth Rock Assurance", "Progressive Corporation", "PURE Insurance", "Quotewizard", "RLI Corp",
            "Root Insurance", "SafeAuto", "Safeco Insurance", "Safeway Insurance Group", "Selective Insurance Group",
            "Shelter Insurance", "Southern Farm Bureau Casualty Insurance Company", "State Auto Insurance",
            "Stillwater Insurance Group", "The General", "The Hanover Insurance Group", "The Travelers Companies",
            "Tower Hill Insurance Group", "Trexis Insurance", "Universal Property  Casualty Insurance",
            "USAA", "Utica National Insurance Group", "Vermont Mutual Insurance Group", "Wawanesa Insurance",
            "West Bend Mutual Insurance Company", "Western General Insurance Company", "Westfield Insurance",
            "Windhaven Insurance", "Workmens Auto Insurance Company", "21st Century Insurance", "4 Ever Life Insurance",
            "AAA Life Insurance", "AARP", "Acuity Insurance", "Aetna", "Allianz Life",
            "Allstate Life Insurance Company", "American Continental Insurance Company", "American Family Life Assurance Company of Columbus",
            "American Fidelity Assurance", "American General Life Insurance", "American Income Life Insurance",
            "American National Insurance Company", "Ameritas Life Insurance", "Amica Life Insurance Company",
            "Assurity Life Insurance Company", "Athene Annuity  Life Assurance", "Atlantic Coast Life Insurance",
            "AXA Equitable Life Insurance", "Banner Life Insurance", "Bankers Life and Casualty",
            "Bestow", "Brighthouse Financial", "Brightpeak Financial", "Catholic Financial Life", "Centene Corporation",
            "Cincinnati Life Insurance Company", "Colonial Penn Life Insurance", "Companion Life Insurance Company",
            "Country Financial", "Delaware Life", "Equitable Holdings", "Ethos Life", "Everence Financial",
            "Farmers New World Life Insurance", "Fidelity  Guaranty Life", "Fidelity Life Association",
            "First Penn-Pacific Life Insurance", "Foresters Financial", "Forethought Life Insurance Company",
            "General Re Life Corporation", "Gerber Life Insurance Company", "Global Atlantic Financial Group",
            "Globe Life", "Great American Life Insurance Company", "Great-West Life Assurance Company",
            "Guardian Life Insurance Company of America", "Guggenheim Life and Annuity Company", "Haven Life",
            "Humana", "Illinois Mutual Life Insurance Company", "Independent Order of Foresters",
            "Industrial Alliance", "Jackson National Life", "John Hancock Financial", "Kansas City Life Insurance",
            "Knights of Columbus", "Lafayette Life Insurance Company", "Legal  General America",
            "Liberty Bankers Life Insurance Company", "Liberty Life Assurance Company of Boston",
            "Life Insurance Company of North America", "Lincoln Financial Group", "MassMutual", "MetLife",
            "Midland National Life Insurance", "Minnesota Life Insurance Company", "Modern Woodmen of America",
            "Mutual of Omaha", "National Life Group", "Nationwide", "New York Life Insurance Company",
            "North American Company for Life and Health Insurance", "Northwestern Mutual", "OneAmerica",
            "Pacific Life", "Pan-American Life Insurance", "Penn Mutual", "Phoenix Life Insurance",
            "Principal Financial Group", "Primerica", "Protective Life", "Prudential Financial",
            "Reliance Standard Life Insurance", "Royal Neighbors of America"
    };

    private static final String[] FIRST_NAMES = {
            // Western names
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer",
            "Michael", "Linda", "William", "Elizabeth", "David", "Barbara",
            "Joseph", "Susan", "Thomas", "Jessica", "Charles", "Sarah",
            "Christopher", "Karen", "Daniel", "Nancy", "Matthew", "Lisa",
            "Anthony", "Margaret", "Mark", "Betty", "Donald", "Sandra",
            "Paul", "Ashley", "Steven", "Dorothy", "Andrew", "Kimberly",
            // Indian names
            "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai",
            "Reyansh", "Ayaan", "Krishna", "Ishaan", "Neha", "Pooja",
            "Aishwarya", "Ananya", "Diya", "Isha", "Kavya", "Meera",
            // Chinese names
            "Wei", "Fang", "Jian", "Min", "Lei", "Ling",
            "Yue", "Hao", "Tian", "Shan", "Mei", "Li",
            "Ping", "Zhen", "Chun", "Dong", "Xia", "Yan"
    };

    private static final String[] LAST_NAMES = {
            // Western names
            "Smith", "Johnson", "Williams", "Jones", "Brown", "Davis",
            "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas",
            "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia",
            "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee",
            "Walker", "Hall", "Allen", "Young", "Hernandez", "King",
            // Indian names
            "Patel", "Sharma", "Reddy", "Nair", "Khan", "Singh",
            "Gupta", "Jain", "Rao", "Chopra", "Iyer", "Kapoor",
            "Menon", "Verma", "Bansal", "Joshi", "Mehta", "Desai",
            // Chinese names
            "Wang", "Li", "Zhang", "Liu", "Chen", "Yang",
            "Huang", "Zhao", "Wu", "Zhou", "Xu", "Sun",
            "Ma", "Zhu", "Hu", "Guo", "Lin", "He"
    };

    private static final String[] MIDDLE_NAMES = {
            "Allen", "Marie", "Lee", "Anne", "Lynn", "Louise",
            "Jane", "Grace", "James", "John", "Michael", "Elizabeth",
            "Marie", "Lee", "Paul", "Ray", "Lynn", "Ann",
            "Jean", "Renee", "Joseph", "Alexander", "Edward", "Marie",
            "Daniel", "Ryan", "Nicole", "Rose", "Thomas", "William"
    };


    private static final String[] INDIAN_CITIES = {
            "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Ahmedabad", "Chennai",
            "Kolkata", "Surat", "Pune", "Jaipur"
    };

    private static final String[] INDIAN_DISTRICTS = {
            "Andheri", "Dwarka", "Whitefield", "Gachibowli", "Bopal",
            "Adyar", "Salt Lake", "Athwa", "Baner", "Malviya Nagar"
    };

    private static final String[] US_CITIES = {
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
            "San Antonio", "San Diego", "Dallas", "San Jose"
    };

    private static final String[] US_DISTRICTS = {
            "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island",
            "Hollywood", "Beverly Hills", "Greenwich", "Downtown", "Uptown"
    };

    private static final String[] CHINESE_CITIES = {
            "Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu",
            "Hangzhou", "Wuhan", "Xian", "Nanjing", "Tianjin"
    };

    private static final String[] CHINESE_DISTRICTS = {
            "Chaoyang", "Huangpu", "Tianhe", "Nanshan", "Jinjiang",
            "Xihu", "Jianghan", "Yanta", "Gulou", "Heping"
    };

    private static final String[] JAPANESE_CITIES = {
            "Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya",
            "Sapporo", "Kobe", "Fukuoka", "Hiroshima", "Sendai"
    };

    private static final String[] JAPANESE_DISTRICTS = {
            "Shibuya", "Umeda", "Gion", "Minato Mirai", "Sakae",
            "Susukino", "Kobe Harborland", "Hakata", "Motomachi", "Aoba"
    };

    private static final String[] STREET_NAMES = {
            "MG Road", "Brigade Road", "Park Street", "Link Road", "Nehru Street",
            "Church Street", "Station Road", "Mall Road", "Temple Road", "Beach Road",
            "Main St", "High St", "Park Ave", "Broadway", "Sunset Blvd",
            "Maple St", "Oak St", "Pine St", "Elm St", "Cedar St",
            "Renmin Road", "Nanjing Road", "Beijing Road", "Tianfu Avenue", "West Lake Road",
            "Sakura Street", "Ginza", "Harajuku Street", "Namba Street", "Kawaramachi"
    };

    private static final Random RANDOM = new Random();
    private static final Random RANDOM_NAMES = new Random();

    public static void main(String[] args) {

        try {
            generateRandomCSVFiles(MAX_FILE_SIZE_MB, MAX_FILES);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getCurrentFileName() {
        LocalDateTime now = LocalDateTime.now();
        return now.format(ISO8601_FORMATFORFILE);
    }

    public static void generateRandomCSVFiles(int maxFileSizeMB, int maxFiles) throws IOException {
        int fileCount = 0;
        int lineCount = 0;
        while (fileCount < maxFiles) {
            String fileName = getCurrentFileName() + ".csv";
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
                while (Files.size(Paths.get(fileName)) < maxFileSizeMB * 1024 * 1024) {

                    String val = String.join(",",generateUUID(),generateRandomName(),
                            generateRandomAddress(),getRandomInsuranceCompany()
                            ,getRandomSex(),String.valueOf(getRandomSalary()),
                            String.valueOf(getRandomAge()),
                            getRandomDouble(),
                            getRandomTimestamp(), getCurrentTimestamp());
                    writer.write(val);
                    writer.newLine();
                    lineCount++;
                }
            }
            System.out.println("Created file: " + fileName + " with " + lineCount + " lines.");
            fileCount++;
            lineCount = 0;
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static int getRandomAge() {
        return RANDOM.nextInt(71) + 10; // Age between 10 and 80
    }

    public static String getRandomSex() {
        return RANDOM.nextBoolean() ? "M" : "F";
    }

    public static int getRandomSalary() {
        return RANDOM.nextInt(Integer.MAX_VALUE - 200) + 200;
    }

    public static String generateRandomName() {
        String firstName = FIRST_NAMES[RANDOM_NAMES.nextInt(FIRST_NAMES.length)];
        String lastName = LAST_NAMES[RANDOM_NAMES.nextInt(LAST_NAMES.length)];

        // Randomly decide if it will be a 2-word or 3-word name
        boolean isThreeWordName = RANDOM_NAMES.nextBoolean();

        if (isThreeWordName) {
            String middleName = MIDDLE_NAMES[RANDOM_NAMES.nextInt(MIDDLE_NAMES.length)];
            return String.format("%s %s %s", firstName, middleName, lastName);
        } else {
            return String.format("%s %s", firstName, lastName);
        }
    }

    public static String generateUUID() {
        return UUID.randomUUID().toString();
    }

    public static String getCurrentTimestamp() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
        return now.format(formatter);
    }

    public static String getRandomTimestamp() {
        long startEpoch = LocalDateTime.of(1973, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long endEpoch = LocalDateTime.of(2024, 12, 31, 23, 59, 59).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long randomEpoch = startEpoch + (long) (RANDOM.nextDouble() * (endEpoch - startEpoch));
        Instant randomInstant = Instant.ofEpochMilli(randomEpoch);
        return ISO8601_FORMAT.format(randomInstant.atZone(ZoneId.systemDefault()).toLocalDateTime());
    }

    public static String getRandomInsuranceCompany() {
        return INSURANCE_COMPANIES[RANDOM.nextInt(INSURANCE_COMPANIES.length)];
    }

    public static String getRandomDouble() {
        int integerPart = RANDOM.nextInt(100000); // Integer part between 0 and 99999
        int decimalPart = RANDOM.nextInt(1000); // Decimal part between 0 and 999
        double randomDouble = integerPart + decimalPart / 1000.0;
        return DECIMAL_FORMAT.format(randomDouble);
    }

    public static String generateRandomAddress() {
        int flatNumber = RANDOM.nextInt(1000) + 1;
        int houseNumber = RANDOM.nextInt(1000) + 1;
        String streetName = STREET_NAMES[RANDOM.nextInt(STREET_NAMES.length)];
        int countrySelector = RANDOM.nextInt(4);

        String cityOrDistrict;
        String country;

        switch (countrySelector) {
            case 0:
                cityOrDistrict = (RANDOM.nextBoolean() ? INDIAN_CITIES : INDIAN_DISTRICTS)[RANDOM.nextInt(INDIAN_CITIES.length)];
                country = "India";
                break;
            case 1:
                cityOrDistrict = (RANDOM.nextBoolean() ? US_CITIES : US_DISTRICTS)[RANDOM.nextInt(US_CITIES.length)];
                country = "USA";
                break;
            case 2:
                cityOrDistrict = (RANDOM.nextBoolean() ? CHINESE_CITIES : CHINESE_DISTRICTS)[RANDOM.nextInt(CHINESE_CITIES.length)];
                country = "China";
                break;
            case 3:
                cityOrDistrict = (RANDOM.nextBoolean() ? JAPANESE_CITIES : JAPANESE_DISTRICTS)[RANDOM.nextInt(JAPANESE_CITIES.length)];
                country = "Japan";
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + countrySelector);
        }

        return String.format("%d %s Apt %d %s %s", houseNumber, streetName, flatNumber, cityOrDistrict, country);
    }
}
