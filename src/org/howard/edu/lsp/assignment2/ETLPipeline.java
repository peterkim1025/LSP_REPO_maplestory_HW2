import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

public class ETLPipeline {

    public static void main(String[] args) {
        String inputPath = "data/products.csv";
        String outputPath = "data/transformed_products.csv";

        List<String[]> transformedData = new ArrayList<>();
        int rowsRead = 0;
        int rowsTransformed = 0;

        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.err.println("Error: Input file '" + inputPath + "' not found. Try again.");
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath))) {
            String headerLine = reader.readLine();

            if (headerLine == null) {
                writeOutput(outputPath, transformedData);
                System.out.println("Input file was empty.");
                printSummary(rowsRead, rowsTransformed, outputPath);
                return;
            }

            transformedData.add(new String[]{"ProductID", "Name", "Price", "Category", "PriceRange"});
            String line;
            while ((line = reader.readLine()) != null) {
                rowsRead++;
                String[] fields = line.split(",");

                if (fields.length != 4) {
                    System.out.println("Skipping malformed line: " + line);
                    continue;
                }

                try {
                    int productId = Integer.parseInt(fields[0]);
                    String name = fields[1].toUpperCase();
                    BigDecimal price = new BigDecimal(fields[2]);
                    String category = fields[3];

                    if (category.equalsIgnoreCase("Electronics")) {
                        price = price.multiply(BigDecimal.valueOf(0.9));
                        price = price.setScale(2, RoundingMode.HALF_UP);

                        if (price.compareTo(BigDecimal.valueOf(500.00)) > 0) {
                            category = "Premium Electronics";
                        }
                    }

                    String priceRange = getPriceRange(price);

                    transformedData.add(new String[]{
                            String.valueOf(productId),
                            name,
                            price.toString(),
                            category,
                            priceRange
                    });

                    rowsTransformed++;

                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    System.out.println("Skipping invalid row: " + line);
                }
            }

            writeOutput(outputPath, transformedData);
            printSummary(rowsRead, rowsTransformed, outputPath);

        } catch (IOException e) {
            System.err.println("Error reading file: " + inputPath);
            e.printStackTrace();
        }
    }

    private static String getPriceRange(BigDecimal price) {
        if (price.compareTo(BigDecimal.valueOf(10.00)) <= 0) {
            return "Low";
        } else if (price.compareTo(BigDecimal.valueOf(100.00)) <= 0) {
            return "Medium";
        } else if (price.compareTo(BigDecimal.valueOf(500.00)) <= 0) {
            return "High";
        } else {
            return "Premium";
        }
    }

    private static void writeOutput(String path, List<String[]> data) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            for (String[] row : data) {
                writer.write(String.join(",", row));
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Error writing to output file: " + path);
            e.printStackTrace();
        }
    }

    private static void printSummary(int rowsRead, int rowsTransformed, String outputPath) {
        int rowsSkipped = rowsRead - rowsTransformed;
        System.out.println("ETL Process Summary:");
        System.out.println("Rows read: " + rowsRead);
        System.out.println("Rows transformed: " + rowsTransformed);
        System.out.println("Rows skipped: " + rowsSkipped);
        System.out.println("Output written to: " + outputPath);
    }
}

