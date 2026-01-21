import { NextResponse } from "next/server";
import { VertexAI } from "@google-cloud/vertexai";

export async function POST(req: Request) {
    try {
        // Parse the form data
        const formData = await req.formData();
        const imageFile = formData.get("image") as File;
        const projectId = formData.get("projectId") as string;

        if (!imageFile) {
            return NextResponse.json(
                { error: "No image file provided" },
                { status: 400 }
            );
        }

        if (!projectId) {
            return NextResponse.json(
                { error: "Project ID is required" },
                { status: 400 }
            );
        }

        // Validate file type
        const validTypes = ["image/png", "image/jpeg", "image/jpg", "image/webp"];
        if (!validTypes.includes(imageFile.type)) {
            return NextResponse.json(
                { error: "Invalid file type. Please upload a PNG, JPG, JPEG, or WebP image." },
                { status: 400 }
            );
        }

        // Validate file size (10MB limit)
        const maxSize = 10 * 1024 * 1024; // 10MB in bytes
        if (imageFile.size > maxSize) {
            return NextResponse.json(
                { error: "File size exceeds 10MB limit" },
                { status: 400 }
            );
        }

        // Convert image to base64
        const bytes = await imageFile.arrayBuffer();
        const buffer = Buffer.from(bytes);
        const base64Image = buffer.toString("base64");

        // Determine the mime type
        const mimeType = imageFile.type;

        // Initialize Vertex AI
        const vertex_ai = new VertexAI({ project: projectId, location: "us-central1" });
        const model = vertex_ai.preview.getGenerativeModel({
            model: "gemini-2.0-flash-exp",
        });

        // Construct the prompt for extracting ERD information
        const prompt = `
You are a Data Model Extraction Expert. Analyze this image which contains a mermaid entity relationship diagram (ERD) or database schema diagram.

Extract and describe the complete data model in a structured format. Include:

1. **Tables/Entities**: List all tables or entities shown in the diagram
2. **Columns/Attributes**: For each table, list all columns with their data types if visible
3. **Primary Keys**: Identify primary key columns for each table
4. **Foreign Keys**: Identify all foreign key relationships between tables
5. **Relationships**: Describe the relationships (one-to-one, one-to-many, many-to-many)
6. **Constraints**: Note any constraints like NOT NULL, UNIQUE, etc. if visible
7. **Additional Details**: Any other relevant information about the data model

Format your response as a clear, detailed description that can be used to generate SQL test cases. Be specific about table names, column names, and relationships.

If the image is not a clear ERD or database schema diagram, please indicate that and describe what you see instead.
`;

        // Call Gemini Vision API
        const result = await model.generateContent({
            contents: [
                {
                    role: "user",
                    parts: [
                        {
                            text: prompt,
                        },
                        {
                            inline_data: {
                                mime_type: mimeType,
                                data: base64Image,
                            },
                        },
                    ],
                },
            ],
        });

        const response = result.response;
        const extractedText = response.candidates?.[0].content.parts[0].text;

        if (!extractedText) {
            throw new Error("No response from Vertex AI");
        }

        return NextResponse.json({
            success: true,
            extractedText: extractedText,
        });

    } catch (error: any) {
        console.error("API Error:", error);
        return NextResponse.json(
            { error: error.message || "Failed to process image" },
            { status: 500 }
        );
    }
}
